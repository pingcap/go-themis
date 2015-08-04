package themis

import (
	"github.com/pingcap/go-themis/hbase"
	. "gopkg.in/check.v1"
)

type SecondaryLockTestSuit struct{}

var _ = Suite(&SecondaryLockTestSuit{})

func (s *SecondaryLockTestSuit) TestSecondaryLock(c *C) {
	l := newSecondaryLock()
	l.primaryCoordinate = &hbase.ColumnCoordinate{
		Table: []byte("tbl"),
		Row:   []byte("row"),
		Column: hbase.Column{
			Family: []byte("cf"),
			Qual:   []byte("q"),
		},
	}

	b := l.toBytes()

	l2, err := parseLockFromBytes(b)
	c.Assert(err, Equals, nil)
	c.Assert(l2.isPrimary(), Equals, false)

	pc := l2.(*SecondaryLock).getPrimaryColumn()
	c.Assert(string(pc.Family), Equals, "cf")
}
