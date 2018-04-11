package keepalived

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Keepalived provides a object which can elect a leader instance by using MySQL.
type Keepalived struct {
	mu     sync.Mutex
	cfg    *Config
	epoch  chan *Epoch
	resign chan struct{}
	dying  chan struct{}
}

const (
	defaultTTLPeriod = 3
	defaultTTL       = 15 * time.Second
)

// Event represents a event in an epoch.
type Event int8

const (
	LeaderChange Event = iota
	NetworkFault
	UnknownFault
)

type Epoch struct {
	Event   Event
	Session string
}

// CreateTable create the MySQL table for election
func CreateTable(table string, db *sql.DB) (err error) {
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS `" + table + "` (`election` VARCHAR(255) NOT NULL,`lock_session` VARCHAR(255) NOT NULL,`last_index` BIGINT NOT NULL,`create_at` DATETIME NOT NULL,PRIMARY KEY (`election`))ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin;")
	return
}

// NewKeepalived create a Keepalived object.
func NewKeepalived(cfg Config) (*Keepalived, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &Keepalived{
		cfg:    &cfg,
		resign: make(chan struct{}),
	}, nil
}

// Config represents the configuration of the keepalived needed
type Config struct {
	Table         string
	Election      string
	Session       string
	EpochChanSize int
	TTL           time.Duration
	DB            *sql.DB
}

// Validate validate each fields of the Config
func (c *Config) Validate() error {
	if c.Table == "" {
		return errors.New("keepalived: empty table")
	}
	if c.Election == "" {
		return errors.New("keepalived: empty election")
	}
	if c.Session == "" {
		return errors.New("keepalived: empty session")
	}
	if c.TTL == 0 {
		c.TTL = defaultTTL
	}
	if c.DB == nil {
		return errors.New("keepalived: empty db")
	}
	return nil
}

// Election launch a election by the given configuration. When you close the
// cfg.Stop, it will exit the election
func (k *Keepalived) Election() <-chan *Epoch {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.dying != nil {
		return k.epoch
	}

	k.dying = make(chan struct{})
	k.epoch = make(chan *Epoch, k.cfg.EpochChanSize)

	go func(dying <-chan struct{}, epoch chan *Epoch) {
		for {
			k.campaign(dying, epoch)
			select {
			case <-dying:
				close(epoch)
				return
			case <-time.After(k.cfg.TTL/defaultTTLPeriod + time.Second):
			case <-k.resign:
			}
		}
	}(k.dying, k.epoch)

	return k.epoch
}

// Exit notifies the candidate to exit the election.
func (k *Keepalived) Exit() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.dying == nil {
		return
	}
	close(k.dying)
	k.dying = nil
}

// Resign notifies the candidate to resign the election once.
func (k *Keepalived) Resign() {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.dying == nil {
		return
	}
	k.resign <- struct{}{}
}

func (k *Keepalived) campaign(dying <-chan struct{}, epoch chan<- *Epoch) {
	var (
		challenge, netfault bool
		lastindex, count    int64
		lastsession         string
		ttl                 = k.cfg.TTL / defaultTTLPeriod
		tick                = time.NewTimer(0)
	)

	defer func() {
		tick.Stop()
		if lastsession == k.cfg.Session {
			epoch <- &Epoch{Event: LeaderChange}
			k.stepdown()
		}
	}()

	for {
		select {
		case <-tick.C:
		case <-dying:
			return
		case <-k.resign:
			return
		}

		var (
			session string
			index   int64
			err     error
		)

		if lastsession == k.cfg.Session {
			index, session, err = k.lease()
		} else {
			index, session, err = k.acquire(lastindex, challenge)
		}

		if err == nil {
			netfault = false
			switch {
			case session != lastsession:
				epoch <- &Epoch{Event: LeaderChange, Session: session}
				fallthrough
			case index != lastindex:
				lastsession, challenge, lastindex, count = session, false, index, 0
			default:
				count++
				if count >= defaultTTLPeriod-1 {
					challenge = true
				}
			}
		} else if _, ok := err.(net.Error); ok || err == driver.ErrBadConn || err == mysql.ErrInvalidConn {
			if !netfault {
				epoch <- &Epoch{Event: NetworkFault}
			}
			lastsession, challenge, netfault, lastindex, count = "", false, true, 0, 0
		} else if err == sql.ErrNoRows {
			return
		} else {
			epoch <- &Epoch{Event: UnknownFault}
			return
		}
		tick.Reset(ttl)
	}
}

func (k *Keepalived) acquire(lastindex int64, challenge bool) (index int64, session string, err error) {
	var (
		tx       *sql.Tx
		result   sql.Result
		affected int64
	)

	if tx, err = k.cfg.DB.Begin(); err != nil {
		return
	}

	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	// lock the mysql row
	row := tx.QueryRow(
		"SELECT `lock_session`, `last_index` FROM `"+
			k.cfg.Table+
			"` WHERE `election` = ? LIMIT 1 FOR UPDATE;",
		k.cfg.Election)
	if err = row.Scan(&session, &index); err != nil && err != sql.ErrNoRows {
		return
	}

	switch session {
	case "":
		// no one hold the election
		n := time.Now()
		result, err = tx.Exec(
			"INSERT INTO `"+k.cfg.Table+
				"` (`election`, `lock_session`, `last_index`, `create_at`) VALUES(?,?,?,?)"+
				" ON DUPLICATE KEY UPDATE `lock_session` = ?, `last_index`=`last_index`+1, `create_at` = ?;",
			k.cfg.Election, k.cfg.Session, int64(1), n, k.cfg.Session, n)
	case k.cfg.Session:
		// the current session is same with us, we hold this election directly
		result, err = tx.Exec(
			"UPDATE `"+
				k.cfg.Table+
				"` SET `last_index`=`last_index`+1 WHERE `election` = ?;",
			k.cfg.Election)
	default:
		if !challenge || lastindex != index { // someone is helding the election
			return
		}
		// someone lost this election
		result, err = tx.Exec(
			"UPDATE `"+
				k.cfg.Table+
				"` SET `lock_session`=?, `last_index`=`last_index`+1, `create_at`=? WHERE `election` = ?;",
			k.cfg.Session, time.Now(), k.cfg.Election)
	}

	if err != nil {
		return
	}

	if affected, err = result.RowsAffected(); err != nil {
		return
	}

	if affected != 0 {
		index++
		session = k.cfg.Session
	}
	return
}

func (k *Keepalived) lease() (index int64, session string, err error) {
	var (
		tx       *sql.Tx
		result   sql.Result
		affected int64
	)

	if tx, err = k.cfg.DB.Begin(); err != nil {
		return
	}

	defer func() {
		if err == nil {
			err = tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	row := tx.QueryRow(
		"SELECT `lock_session`, `last_index` FROM `"+k.cfg.Table+
			"` WHERE `election` = ? LIMIT 1 FOR UPDATE;",
		k.cfg.Election)
	if err = row.Scan(&session, &index); err != nil {
		return
	}

	if session != k.cfg.Session {
		err = sql.ErrNoRows
		return
	}

	result, err = tx.Exec(
		"UPDATE `"+k.cfg.Table+
			"` SET `last_index`=`last_index`+1 WHERE `election` = ?;",
		k.cfg.Election)
	if err != nil {
		return
	}

	if affected, err = result.RowsAffected(); err != nil {
		return
	}

	if affected == 0 {
		err = errors.New("keepalived: lease missing")
	}
	return
}

func (k *Keepalived) stepdown() {
	var (
		tx  *sql.Tx
		err error
	)

	if tx, err = k.cfg.DB.Begin(); err != nil {
		return
	}

	defer func() {
		if err == nil {
			tx.Commit()
		} else {
			tx.Rollback()
		}
	}()

	_, err = tx.Exec(
		"UPDATE `"+k.cfg.Table+"` SET `lock_session`='' WHERE `election` = ?;",
		k.cfg.Election)
}
