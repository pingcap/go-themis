package main

import (
	"fmt"

	"github.com/ngaut/log"
	"github.com/pingcap/go-themis"
)

func main() {
	log.SetLevelByString("error")
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

	for i := 0; i < 10000; i++ {
		put := themis.CreateNewPut([]byte(fmt.Sprintf("hello_1_%d", i)))
		put.AddValue([]byte("cf"), []byte("v"), []byte("new value"))
		_, err := c.Put("t1", put)
		if err != nil {
			log.Fatal(err)
		}
	}

	get = themis.CreateNewGet([]byte("hello"))
	result, err = c.Get("t1", get)
	if err != nil {
		log.Fatal(err)
	}
	log.Info(result, result.Columns["cf:v"].Value, result.Columns["cf:v"].Timestamp.Unix())
}
