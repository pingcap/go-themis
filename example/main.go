package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"runtime"

	"net/http"
	_ "net/http/pprof"

	"github.com/c4pt0r/go-hbase"
	"github.com/ngaut/log"
	"github.com/pingcap/go-themis"
)

var c hbase.HBaseClient
var tblName = "themis_bench"

func init() {
	var err error
	c, err = hbase.NewClient([]string{"cuiqiu-pc:2222"}, "/hbase")
	if err != nil {
		log.Fatal(err)
	}
}

func createTable() {
	if !c.TableExists(tblName) {
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
}

func dropTable() {
	t := hbase.NewTableNameWithDefaultNS(tblName)
	c.DisableTable(t)
	c.DropTable(t)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	log.SetLevelByString("error")
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

			for j := 0; j < 10; j++ {
				tx := themis.NewTxn(c)

				put := hbase.NewPut([]byte(fmt.Sprintf("1Row_%d_%d", i, j)))
				put.AddValue([]byte("cf"), []byte("q"), []byte(strconv.Itoa(i)))

				put2 := hbase.NewPut([]byte(fmt.Sprintf("2Row_%d_%d", i, j)))
				put2.AddValue([]byte("cf"), []byte("q"), []byte(strconv.Itoa(i)))

				put3 := hbase.NewPut([]byte(fmt.Sprintf("3Row_%d_%d", i, j)))
				put3.AddValue([]byte("cf"), []byte("q"), []byte(strconv.Itoa(i)))

				put4 := hbase.NewPut([]byte(fmt.Sprintf("4Row_%d_%d", i, j)))
				put4.AddValue([]byte("cf"), []byte("q"), []byte(strconv.Itoa(i)))

				put5 := hbase.NewPut([]byte(fmt.Sprintf("5Row_%d_%d", i, j)))
				put5.AddValue([]byte("cf"), []byte("q"), []byte(strconv.Itoa(i)))

				put6 := hbase.NewPut([]byte(fmt.Sprintf("6Row_%d_%d", i, j)))
				put6.AddValue([]byte("cf"), []byte("q"), []byte(strconv.Itoa(i)))

				put7 := hbase.NewPut([]byte(fmt.Sprintf("7Row_%d_%d", i, j)))
				put7.AddValue([]byte("cf"), []byte("q"), []byte(strconv.Itoa(i)))

				put8 := hbase.NewPut([]byte(fmt.Sprintf("8Row_%d_%d", i, j)))
				put8.AddValue([]byte("cf"), []byte("q"), []byte(strconv.Itoa(i)))

				tx.Put(tblName, put)
				tx.Put(tblName, put2)
				tx.Put(tblName, put3)
				tx.Put(tblName, put4)
				tx.Put(tblName, put5)
				tx.Put(tblName, put6)
				tx.Put(tblName, put7)
				tx.Put(tblName, put8)

				err := tx.Commit()
				if err != nil {
					log.Error(err)
				}
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(ct)
	log.Errorf("took %s", elapsed)
}
