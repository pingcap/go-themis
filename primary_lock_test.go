package themis

import (
	"testing"

	"github.com/pingcap/go-hbase"

	. "gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type ThemisPrimaryLockTestSuit struct{}

var _ = Suite(&ThemisPrimaryLockTestSuit{})

func (s *ThemisPrimaryLockTestSuit) TestPrimaryLock(c *C) {
	l := newPrimaryLock()

	cc := &hbase.ColumnCoordinate{
		Table:  []byte("tbl"),
		Row:    []byte("row"),
		Column: hbase.Column{[]byte("cf"), []byte("q")},
	}

	l.ts = 1024
	l.addSecondaryColumn(cc, hbase.TypePut)
	l.setColumn(cc)

	c.Assert(l.isPrimary(), Equals, true)

	b := l.toBytes()
	lock, err := parseLockFromBytes(b)

	c.Assert(lock.(*PrimaryLock).getSecondaryColumnType(cc), Equals, hbase.TypePut)
	c.Assert(err, Equals, nil)
	c.Assert(lock.isPrimary(), Equals, true)
	c.Assert(lock.getTimestamp(), Equals, uint64(1024))
	c.Assert(lock.isExpired(), Equals, false)
}
