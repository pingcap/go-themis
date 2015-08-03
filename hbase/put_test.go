package hbase

import (
	"bytes"

	"github.com/pingcap/go-themis/proto"
	. "gopkg.in/check.v1"
)

type HBasePutTestSuit struct{}

var _ = Suite(&HBasePutTestSuit{})

func (s *HBasePutTestSuit) TestPut(c *C) {
	g := CreateNewPut([]byte("row"))
	g.AddValue([]byte("cf"), []byte("q"), []byte("val"))
	msg := g.ToProto()
	p, _ := msg.(*proto.MutationProto)

	c.Assert(*p.MutateType, Equals, *proto.MutationProto_PUT.Enum())

	for _, col := range p.ColumnValue {
		for _, v := range col.QualifierValue {
			c.Assert(bytes.Compare([]byte("q"), v.Qualifier), Equals, 0)
			c.Assert(bytes.Compare([]byte("val"), v.Value), Equals, 0)
		}
	}
}
