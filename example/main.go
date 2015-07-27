package main

import (
	"github.com/ngaut/log"
	"github.com/pingcap/go-themis"
)

func main() {
	c, err := themis.NewClient([]string{"localhost"}, "/hbase")
	if err != nil {
		log.Fatal(err)
	}
	get := themis.CreateNewGet([]byte("hello"))
	result, err := c.Get("t1", get)
	if err != nil {
		log.Fatal(err)
	}
	log.Info(result, result.Columns["cf:v"].Value)
}
