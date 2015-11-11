package main

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"runtime"

	"net/http"
	_ "net/http/pprof"

	"flag"

	"github.com/ngaut/log"
	"github.com/pingcap/go-hbase"
	"github.com/pingcap/go-themis"
)

var c hbase.HBaseClient
var tblName = "themis_bench"

var (
	zk = flag.String("zk", "localhost", "hbase zookeeper info")
)

func getZkHosts() []string {
	zks := strings.Split(*zk, ",")
	if len(zks) == 0 {
		log.Fatal("invalid zk")
	}
	return zks
}

func createHBaseClient(zk string) error {
	var err error
	c, err = hbase.NewClient(getZkHosts(), "/hbase")
	return err
}

func createTable() {
	// create new hbase table for store
	t := hbase.NewTableDesciptor(hbase.NewTableNameWithDefaultNS(tblName))
	cf := hbase.NewColumnFamilyDescriptor("cf")
	cf.AddStrAddr("THEMIS_ENABLE", "true")
	t.AddColumnDesc(cf)
	err := c.CreateTable(t, nil)
	if err != nil {
		log.Fatal(err)
	}
}

func dropTable() {
	t := hbase.NewTableNameWithDefaultNS(tblName)
	c.DisableTable(t)
	c.DropTable(t)
}

func main() {
	flag.Parse()
	prefix := fmt.Sprintf("%v", time.Now().UnixNano())
	err := createHBaseClient(*zk)
	if err != nil {
		log.Warn("argument zk : modify hbase zk address and port, example: -zk=cuiqiu-pc:2222")
		log.Fatal(err)
		return
	}

	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetLevelByString("warn")
	dropTable()
	createTable()

	go func() {
		log.Error(http.ListenAndServe("localhost:8889", nil))
	}()

	ct := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < 5000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			tx := themis.NewTxn(c)

			put := hbase.NewPut([]byte(fmt.Sprintf("1Row_%s_%d", prefix, i)))
			put.AddValue([]byte("cf"), []byte("q"), []byte(strconv.Itoa(i)))

			put2 := hbase.NewPut([]byte(fmt.Sprintf("2Row_%s_%d", prefix, i)))
			put2.AddValue([]byte("cf"), []byte("q"), []byte(strconv.Itoa(i)))

			put3 := hbase.NewPut([]byte(fmt.Sprintf("3Row_%s_%d", prefix, i)))
			put3.AddValue([]byte("cf"), []byte("q"), []byte(strconv.Itoa(i)))

			put4 := hbase.NewPut([]byte(fmt.Sprintf("4Row_%s_%d", prefix, i)))
			put4.AddValue([]byte("cf"), []byte("q"), []byte(strconv.Itoa(i)))

			tx.Put(tblName, put)
			tx.Put(tblName, put2)
			tx.Put(tblName, put3)
			tx.Put(tblName, put4)

			err := tx.Commit()
			if err != nil {
				log.Error(err)
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(ct)
	log.Errorf("took %s", elapsed)
}
