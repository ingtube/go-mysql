package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/siddontang/go-mysql/canal"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"

	"github.com/siddontang/go-log/log"
)

func main() {
	log.SetLevel(log.LevelError)
	flag.Parse()

	cfg := canal.NewDefaultConfig()
	cfg.Addr = "xxx"
	cfg.User = "xxx"
	cfg.Password = "xxx"
	cfg.UseDecimal = true

	cfg.ReadTimeout = 90 * time.Second
	cfg.HeartbeatPeriod = 60 * time.Second
	cfg.ServerID = 1
	cfg.Dump.ExecutionPath = ""
	cfg.Dump.DiscardErr = false

	c, err := canal.NewCanal(cfg)
	if err != nil {
		fmt.Printf("create canal err %v", err)
		os.Exit(1)
	}

	//if len(*ignoreTables) > 0 {
	//	subs := strings.Split(*ignoreTables, ",")
	//	for _, sub := range subs {
	//		if seps := strings.Split(sub, "."); len(seps) == 2 {
	//			c.AddDumpIgnoreTables(seps[0], seps[1])
	//		}
	//	}
	//}

	//if len(*tables) > 0 && len(*tableDB) > 0 {
	//	subs := strings.Split(*tables, ",")
	//	c.AddDumpTables(*tableDB, subs...)
	//} else if len(*dbs) > 0 {
	//	subs := strings.Split(*dbs, ",")
	//	c.AddDumpDatabases(subs...)
	//}

	c.SetEventHandler(&handler{})

	startPos := mysql.Position{
		Name: "mysql-bin.000681",
		Pos:  218856,
	}
	go func() {
		err = c.RunFrom(startPos)
		if err != nil {
			fmt.Printf("start canal err %v", err)
		}
	}()

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		os.Kill,
		os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	<-sc

	c.Close()
}

type handler struct {
	canal.DummyEventHandler
}

// 打印position
func (h *handler) OnXID(p mysql.Position) error {
	return nil
}
func (h *handler) OnGTID(p mysql.GTIDSet) error {
	return nil
}

var a int

func (h *handler) OnTransBegin(queryEvent *replication.QueryEvent) error {
	//panic(1)
	log.Error(queryEvent.SlaveProxyID)
	time.Sleep(time.Second)
	a++
	if a%2 == 0 {
		log.Error("error")
		//return errors.New("123")
	}
	return nil
}
// 解析 发送至mq
func (h *handler) OnRow(e *canal.RowsEvent) error {
	//log.Error(e.Header.Timestamp)
	//return nil
	log.Errorf("数据类型:%s,table:%s,time:%d", e.Action, e.Table, e.Header.Timestamp)
	//for i := range e.Table.Columns {
	//if e.Action == "update" {
	//	if e.Rows[0][i] != e.Rows[1][i] {
	//		log.Println(e.Table.Columns[i].Name, ":", e.Rows[0][i], " ")
	//	}
	//}
	//fmt.Print(e.Table.Columns[i].Name, ":", e.Rows[0][i], " ")
	//if len(e.Rows) > 1 {
	//	fmt.Print("after:", e.Rows[1][i], " ")
	//}
	//}
	//fmt.Println()

	return nil
}
func (h *handler) OnRotate(*replication.RotateEvent) error          { return nil }
func (h *handler) OnTableChanged(schema string, table string) error { return nil }

// TODO: 可记录至mysql
func (h *handler) OnPosSynced(p mysql.Position, s mysql.GTIDSet, b bool) error {
	return nil
}

func (h *handler) String() string {
	return "TestHandler"
}
