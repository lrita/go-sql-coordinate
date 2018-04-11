package keepalived

import (
	"database/sql"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

var dsn = "root@tcp(127.0.0.1:3306)/test"

const ttable = "keepalived_test_table"

func init() {
	if n := os.Getenv("TEST_KEEPALIVED_DSN"); n != "" {
		dsn = n
	}
}

type fsuite struct {
	suite.Suite
	db *sql.DB
}

func (s *fsuite) SetupSuite() {
	db, err := sql.Open("mysql", dsn+"?charset=utf8&parseTime=True&loc=Local&timeout=1s&writeTimeout=3s&readTimeout=3s")
	s.Require().NoError(err)
	s.db = db
}

func (s *fsuite) TearDownSuite() {
	_, err := s.db.Exec("DROP TABLE IF EXISTS `" + ttable + "`;")
	s.Assert().NoError(err)
	s.Require().NoError(s.db.Close())
}

func (s *fsuite) TestKeepalivedBase() {
	var (
		cfg    Config
		kp     *Keepalived
		assert = s.Assert()
	)

	_, err := NewKeepalived(cfg)
	assert.Error(err)

	cfg.Table = "election"
	_, err = NewKeepalived(cfg)
	assert.Error(err)

	cfg.Election = "abcd"
	_, err = NewKeepalived(cfg)
	assert.Error(err)

	cfg.Session = "qqq"
	_, err = NewKeepalived(cfg)
	assert.Error(err)

	cfg.DB = s.db
	kp, err = NewKeepalived(cfg)
	assert.NoError(err)
	kp.Exit()
}

func (s *fsuite) TestCreateTable() {
	var (
		db     string
		count  int
		assert = s.Assert()
	)

	hastable := func() bool {
		assert.NoError(s.db.QueryRow("SELECT DATABASE()").Scan(&db))
		assert.NoError(s.db.QueryRow(
			"SELECT count(*) FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ? AND table_name = ?",
			db, ttable).Scan(&count))
		return count > 0
	}

	_, err := s.db.Exec("DROP TABLE IF EXISTS `" + ttable + "`;")
	assert.NoError(err)
	assert.False(hastable())
	assert.NoError(CreateTable(ttable, s.db))
	assert.True(hastable())
	assert.NoError(CreateTable(ttable, s.db))
	assert.True(hastable())
}

func (s *fsuite) TestKeepalivedElection() {
	const election = "test_election"
	var (
		wg            sync.WaitGroup
		election0done = make(chan struct{})
		election1done = make(chan struct{})
		election2done = make(chan struct{})
		election3done = make(chan struct{})
	)

	keepalived0, err := NewKeepalived(Config{
		Table:         ttable,
		Election:      election,
		Session:       "keepalived0",
		EpochChanSize: 10,
		DB:            s.db,
	})
	s.Require().NoError(err)

	keepalived1, err := NewKeepalived(Config{
		Table:         ttable,
		Election:      election,
		Session:       "keepalived1",
		EpochChanSize: 10,
		DB:            s.db,
	})
	s.Require().NoError(err)

	wg.Add(1)
	go func() {
		defer wg.Done()
		n := 0
		ch := keepalived0.Election()
		for e := range ch {
			switch n {
			case 0:
				s.Assert().Equal(LeaderChange, e.Event)
				s.Assert().Equal("keepalived0", e.Session)
				close(election0done)
			case 1:
				s.Assert().Equal(LeaderChange, e.Event)
				s.Assert().Equal("", e.Session)
				close(election2done)
			}
			n++
		}
	}()

	select {
	case <-election0done:
	case <-time.After(defaultTTL):
		s.T().Error("keepalived0 election timeout")
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		n := 0
		ch := keepalived1.Election()
		for e := range ch {
			switch n {
			case 0:
				s.Assert().Equal(LeaderChange, e.Event)
				s.Assert().Equal("keepalived0", e.Session)
				close(election1done)
			case 1:
				s.Assert().Equal(LeaderChange, e.Event)
				s.Assert().Equal("keepalived1", e.Session)
				close(election3done)
			}
			n++
		}
	}()

	select {
	case <-election1done:
	case <-time.After(defaultTTL):
		s.T().Error("keepalived1 watch election timeout")
	}

	keepalived0.Resign()

	select {
	case <-election2done:
	case <-time.After(defaultTTL):
		s.T().Error("keepalived0 stepdown timeout")
	}

	select {
	case <-election3done:
	case <-time.After(defaultTTL):
		s.T().Error("keepalived1 become leader timeout")
	}

	time.Sleep(defaultTTL)

	keepalived1.Exit()
	keepalived0.Exit()

	wg.Wait()
}

func TestKeepalived(t *testing.T) {
	suite.Run(t, &fsuite{})
}
