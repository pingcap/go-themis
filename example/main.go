package main

import (
	"github.com/ngaut/log"
	"github.com/pingcap/go-themis"
	"github.com/pingcap/go-themis/hbase"
)

func main() {
	log.SetLevelByString("info")
	c, err := themis.NewClient([]string{"localhost"}, "/hbase")
	if err != nil {
		log.Fatal(err)
	}
	get := hbase.CreateNewGet([]byte("hello"))
	result, err := c.Get("t1", get)
	if err != nil {
		log.Fatal(err)
	}
	log.Info(result, result.Columns["cf:v"].Value, result.Columns["cf:v"].Ts)

	tx := themis.NewTxn(c)

	get = hbase.CreateNewGet([]byte("Joe"))
	get.AddColumn([]byte("Account"), []byte("cash"))
	tx.Get("CashTable", get)

	put := hbase.CreateNewPut([]byte("Joe"))
	put.AddValue([]byte("Account"), []byte("cash"), []byte("1"))

	put2 := hbase.CreateNewPut([]byte("Bob"))
	put2.AddValue([]byte("Account"), []byte("cash"), []byte("2"))

	tx.Put("CashTable", put)
	tx.Put("CashTable", put2)

	err = tx.Commit()
	if err != nil {
		log.Error(err)
	}

}
