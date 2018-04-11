# go-sql-coordinate
In some cases, we have some instances in a cluster and we want run a special function
in only one instance. We want not hardcode the instance, because it maybe crash. So we
want those instances in the cluster can auto elect a leader. Usually, we can use etcd/consul
to do this. But some time we only use mysql in the code. We want not maintain a etcd/consul
cluster only for a special function. We can use mysql do this.

# example
```go
package main

import (
    "database/sql"
    "log"

    "github.com/go-sql-driver/mysql"
    "github.com/lrita/go-sql-coordinate/keepalived"
)

func main() {
    // Recommend set a timeout with mysql, which will make it sensitive when network broken down.
    db, err := sql.Open("mysql", "root@tcp(127.0.0.1:3306)/test?charset=utf8&parseTime=True&loc=Local&timeout=1s&writeTimeout=3s&readTimeout=3s)
    if err != nil {
        log.Fatal(err)
    }

    tablename := "test_election"
    session := "127.0.0.1:80"
    if err := keepalived.CreateTable(tablename, db); err != nil {
        log.Fatal(err)
    }

    keepa, err := keepalived.NewKeepalived(keepalived.Config{
        Table:         tablename,
        Election:      "cluster_leader",
        Session:       session,
        EpochChanSize: 10,
        DB:            db,
    })
    if err != nil {
        log.Fatal(err)
    }

    epoch := keepa.Election()
    go func() {
        for e := range epoch {
            if e.Session == session {
                log.Printf("I am become the leader of cluster_leader")
            } else {
                log.Printf("leader change to %s", e.Session)
            }
        }
    }()

    // do something...
    keepa.Exit()
}
```
