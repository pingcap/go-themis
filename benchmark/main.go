package main

import (
	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"runtime"

	"net/http"
	_ "net/http/pprof"

	"github.com/ngaut/log"
	"github.com/pingcap/go-hbase"
	"github.com/pingcap/go-themis"
)

var c hbase.HBaseClient
var tblName1 = "themis_1"
var tblName2 = "themis_2"

// some comments
func init() {
	log.Errorf("create conn")
	var err error
	c, err = hbase.NewClient([]string{"localhost"}, "/hbase")
	if err != nil {
		log.Fatal(err)
	}
	log.Errorf("create conn done")
}

func createTable(tblName string) {
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

func dropTable(tblName string) {
	t := hbase.NewTableNameWithDefaultNS(tblName)
	c.DisableTable(t)
	c.DropTable(t)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetLevelByString("debug")
	log.Errorf("begin create table")
	dropTable(tblName1)
	createTable(tblName1)
	dropTable(tblName2)
	createTable(tblName2)
	log.Errorf("create table finish")

	go func() {
		log.Error(http.ListenAndServe("localhost:8889", nil))
	}()

	batchInsert(5, 50000)
}

func batchInsert(round int, rowCount int) {
	for i := 0; i < round; i++ {
		log.Errorf("begin batch insert")
		ct := time.Now()
		tx := themis.NewTxn(c)
		for j := 0; j < rowCount; j++ {
			put := hbase.NewPut([]byte(fmt.Sprintf("Row_%d_%d", j, i)))
			put.AddValue([]byte("cf"), []byte("q"), bytes.Repeat([]byte{'A'}, 512))
			tx.Put(tblName1, put)
		}
		err := tx.Commit()
		if err != nil {
			log.Error(err)
		}
		log.Errorf("insert %d row data, consum time %s", rowCount, time.Since(ct))
	}
}

func insert(rowCount int) {
	ct := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < rowCount; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()

			tx := themis.NewTxn(c)

			put := hbase.NewPut([]byte(fmt.Sprintf("Row_%d", i)))
			put.AddValue([]byte("cf"), []byte("q"), []byte(strconv.Itoa(i)))

			put2 := hbase.NewPut([]byte(fmt.Sprintf("SRow_%d", i)))
			put2.AddValue([]byte("cf"), []byte("q"), []byte(strconv.Itoa(i)))

			tx.Put(tblName1, put)
			tx.Put(tblName2, put2)

			err := tx.Commit()
			if err != nil {
				log.Error(err)
			}
		}(i)
	}

	wg.Wait()
	log.Errorf("insert %d row data, consum time %s", rowCount, time.Since(ct))
}

func randomGet(rowCount int) {
	ct := time.Now()
	wg := sync.WaitGroup{}

	for i := 0; i < rowCount; i++ {
		wg.Add(1)
		go func(count int) {
			defer wg.Done()

			tx := themis.NewTxn(c)
			rowKey := fmt.Sprintf("Row_%d", rand.Intn(rowCount))
			get := hbase.NewGet([]byte(rowKey))
			value, err := tx.Get(tblName1, get)
			if err != nil {
				log.Errorf("get rowkey: %s, has a error", rowKey, err)
			} else {
				log.Errorf("get rowkey: %s, value:%s", rowKey, value)
			}
		}(rowCount)
	}

	wg.Wait()
	log.Errorf("random get %d row data, consum time %s", rowCount, time.Since(ct))
}
