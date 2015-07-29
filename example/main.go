package main

import (
	"github.com/ngaut/log"
	"github.com/pingcap/go-themis"
)

func main() {
	log.SetLevelByString("info")
	c, err := themis.NewClient([]string{"localhost"}, "/hbase")
	if err != nil {
		log.Fatal(err)
	}
	get := themis.CreateNewGet([]byte("hello"))
	result, err := c.Get("t1", get)
	if err != nil {
		log.Fatal(err)
	}
	log.Info(result, result.Columns["cf:v"].Value, result.Columns["cf:v"].Timestamp.Unix())

	get = themis.CreateNewGet([]byte("Joe"))
	get.AddColumn([]byte("Account"), []byte("cash"))
	tx := themis.NewTxn(c)
	themisGet := themis.NewThemisGet(get)
	tx.Get("CashTable", themisGet)

	put := themis.CreateNewPut([]byte("Joe"))
	put.AddValue([]byte("Account"), []byte("cash"), []byte("1"))

	put2 := themis.CreateNewPut([]byte("Bob"))
	put2.AddValue([]byte("Account"), []byte("cash"), []byte("2"))

	tx.Put("CashTable", themis.NewThemisPut(put))
	tx.Put("CashTable", themis.NewThemisPut(put2))

	tx.Commit()

}
