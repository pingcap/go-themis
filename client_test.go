package themis

import (
	"github.com/ngaut/log"
	"github.com/pingcap/go-themis/hbase"
	. "gopkg.in/check.v1"
)

type HBaseClientTestSuit struct{}

var _ = Suite(&HBaseClientTestSuit{})

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
	c.Assert(result != nil, Equals, true)

	//c.Assert(bytes.Compare(result.Columns["cf:q"].Value, []byte("v")) == 0, Equals, true)

	scan := newScan([]byte("t1"), cli)
	scan.StartRow = []byte("hello_\x00")
	scan.StopRow = []byte("hello_\xff")

	for {
		r := scan.Next()

		if r == nil || scan.closed {
			break
		}

		log.Error(string(r.Row))
	}

	del := hbase.CreateNewDelete([]byte("row"))
	del.AddFamily([]byte("cf"))
	ok, err = cli.Delete("t1", del)

	c.Assert(ok, Equals, true)
	c.Assert(err, Equals, nil)
}
