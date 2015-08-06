package themis

import (
	"bytes"

	"github.com/ngaut/log"
	"github.com/pingcap/go-themis/hbase"
	. "gopkg.in/check.v1"
)

type HBaseClientTestSuit struct{}

var _ = Suite(&HBaseClientTestSuit{})

const (
	DefaultSeparator byte = ':'
)

func (s *HBaseClientTestSuit) TestHBaseClient(c *C) {
	cli, err := NewClient([]string{"localhost"}, "/hbase")
	c.Assert(err, Equals, nil)

	put := hbase.CreateNewPut([]byte("row"))
	put.AddStringValue("cf", "q", "v")
	ok, err := cli.Put("t1", put)
	c.Assert(ok, Equals, true)
	c.Assert(err, Equals, nil)

	get := hbase.CreateNewGet([]byte("row"))
	get.AddStringColumn("cf", "q")
	result, err := cli.Get("t1", get)
	c.Assert(err, Equals, nil)

	c.Assert(bytes.Compare(result.Columns["cf:q"].Value, []byte("v")) == 0, Equals, true)

	del := hbase.CreateNewDelete([]byte("row"))
	del.AddFamily([]byte("cf"))
	ok, err = cli.Delete("t1", del)

	c.Assert(ok, Equals, true)
	c.Assert(err, Equals, nil)

	scan := newScan([]byte("t1"), cli)

	for {
		results := scan.next()

		if results == nil {
			break
		}

		for _, v := range results {
			log.Info(v)
			if scan.closed {
				return
			}
		}
	}
}
