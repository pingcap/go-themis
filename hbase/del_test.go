package hbase

import (
	"bytes"

	"github.com/pingcap/go-themis/proto"
	. "gopkg.in/check.v1"
)

type HBaseDelTestSuit struct{}

var _ = Suite(&HBaseDelTestSuit{})

func (s *HBaseDelTestSuit) TestDel(c *C) {
	d := CreateNewDelete([]byte("hello"))
	d.AddFamily([]byte("cf"))
	d.AddFamily([]byte("cf1"))
	msg := d.ToProto()

	p, ok := msg.(*proto.MutationProto)
	c.Assert(ok, Equals, true)
	c.Assert(bytes.Compare(p.Row, []byte("hello")), Equals, 0)
	c.Assert(*p.MutateType, Equals, *proto.MutationProto_DELETE.Enum())

	cv := p.GetColumnValue()
	c.Assert(len(cv), Equals, 2)

	for _, v := range cv {
		c.Assert(len(v.QualifierValue), Equals, 1)
		c.Assert(*v.QualifierValue[0].DeleteType, Equals, *proto.MutationProto_DELETE_FAMILY.Enum())
	}

	d = CreateNewDelete([]byte("hello"))
	d.AddStringColumn("cf", "q")
	d.AddStringColumn("cf", "q")
	d.AddStringColumn("cf", "q")
	msg = d.ToProto()
	p, _ = msg.(*proto.MutationProto)
	cv = p.GetColumnValue()
	c.Assert(len(cv), Equals, 1)

	for _, v := range cv {
		c.Assert(len(v.QualifierValue), Equals, 1)
		c.Assert(*v.QualifierValue[0].DeleteType, Equals, *proto.MutationProto_DELETE_MULTIPLE_VERSIONS.Enum())
	}

}
