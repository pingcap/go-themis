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
	log.Info(c)
}
