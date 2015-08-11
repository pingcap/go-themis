package main

import (
	"strconv"
	"sync"

	"runtime"

	"github.com/c4pt0r/go-hbase"
	"github.com/ngaut/log"
	"github.com/pingcap/go-themis"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() / 2)
	log.SetLevelByString("info")
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := hbase.NewClient([]string{"localhost"}, "/hbase")
			if err != nil {
				return
			}

			tx := themis.NewTxn(c)

			get := hbase.NewGet([]byte("Joe"))
			get.AddColumn([]byte("Account"), []byte("cash"))
			tx.Get("CashTable", get)

			put := hbase.NewPut([]byte("Joe"))
			put.AddValue([]byte("Account"), []byte("cash"), []byte(strconv.Itoa(i)))

			put2 := hbase.NewPut([]byte("Bob"))
			put2.AddValue([]byte("Account"), []byte("cash"), []byte(strconv.Itoa(i)))

			tx.Put("CashTable", put)
			tx.Put("CashTable", put2)

			err = tx.Commit()
			if err != nil {
				log.Error(err)
			}
		}(i)
	}
	wg.Wait()
}
