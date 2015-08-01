package hbase

import (
	"bytes"
	"testing"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type ColumnTestSuit struct{}

var _ = Suite(&ColumnTestSuit{})

func (s *ColumnTestSuit) TestColumn(c *C) {
	col := NewColumn([]byte("cf"), []byte("q"))
	c.Assert(bytes.Compare(col.Family, []byte("cf")), Equals, 0)
	c.Assert(bytes.Compare(col.Qual, []byte("q")), Equals, 0)

	colStr := "cf:q"
	col = &Column{}
	col.ParseFromString(colStr)

	c.Assert(bytes.Compare(col.Family, []byte("cf")), Equals, 0)
	c.Assert(bytes.Compare(col.Qual, []byte("q")), Equals, 0)

	buf := bytes.NewBuffer(nil)
	col.Write(buf)
	c.Assert(len(buf.Bytes()), Equals, 5)
}

func (s *ColumnTestSuit) TestColumnCoordinate(c *C) {
	cc := NewColumnCoordinate([]byte("tbl"),
		[]byte("row"), []byte("cf"), []byte("q"))

	buf := bytes.NewBuffer(nil)
	cc.Write(buf)
	c.Assert(len(buf.Bytes()), Equals, 13)

	cc2 := NewColumnCoordinate([]byte("tbl1"),
		[]byte("row"), []byte("cf"), []byte("q"))

	c.Assert(cc.Equal(cc2), Equals, false)
	cc2.Table = []byte("tbl")
	c.Assert(cc.Equal(cc2), Equals, true)

	c.Assert(cc.String(), Equals, "tbl:row:cf:q")

	cc = &ColumnCoordinate{}
	cc.ParseFromString("tbl1:r:cf:qq")
	c.Assert(cc.String(), Equals, "tbl1:r:cf:qq")

	cc.ParseField(bytes.NewBuffer(buf.Bytes()))
	c.Assert(cc.String(), Equals, "tbl:row:cf:q")

}
