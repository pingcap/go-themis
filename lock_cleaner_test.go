package themis

import (
	"bytes"
	"encoding/binary"
	"math"

	"github.com/c4pt0r/go-hbase"
	. "gopkg.in/check.v1"
)

type LockCleanerTestSuit struct{}

var _ = Suite(&LockCleanerTestSuit{})

func generateTestColumn() *hbase.Column {
	return &hbase.Column{
		Family: []byte("cf"),
		Qual:   []byte("q"),
	}
}
func generateTestColCoordinate() *hbase.ColumnCoordinate {
	return &hbase.ColumnCoordinate{
		Table: []byte("tbl"),
		Row:   []byte("row"),
		Column: hbase.Column{
			Family: []byte("cf"),
			Qual:   []byte("q"),
		},
	}
}

func (s *LockCleanerTestSuit) buildMockThemis() *mockThemisClient {
	mockThemis := &mockThemisClient{}
	cc := generateTestColCoordinate()
	mockThemis.On("getLockAndErase", cc, uint64(1024)).Return(nil, nil)
	return mockThemis
}

func (s *LockCleanerTestSuit) buildMockHbase() *mockHbaseClient {
	mockHbase := &mockHbaseClient{}

	// should call these
	g := hbase.NewGet([]byte("row"))
	g.AddStringColumn("#p", "cf#q")
	g.AddStringColumn("#d", "cf#q")
	// [1024, inf)
	g.AddTimeRange(1024, math.MaxInt64)
	// target
	buf := bytes.NewBuffer(nil)
	binary.Write(buf, binary.BigEndian, uint64(1024))

	res := &hbase.ResultRow{
		SortedColumns: []*hbase.Kv{
			&hbase.Kv{
				Ts:    uint64(2048),
				Value: buf.Bytes(),
			},
		},
	}

	mockHbase.On("Get", "tbl", g).Return(res, nil)
	return mockHbase
}

func (s *LockCleanerTestSuit) TestLockCleanerEraseData(c *C) {
	mockThemis := &mockThemisClient{}
	mockHbase := &mockHbaseClient{}

	col := hbase.Column{
		Family: []byte("cf"),
		Qual:   []byte("q"),
	}

	d := hbase.NewDelete([]byte("row"))
	d.AddColumnWithTimestamp([]byte("cf"), []byte("q"), 0)
	d.AddColumnWithTimestamp([]byte("L"), []byte("cf#q"), 0)
	mockHbase.On("Delete", "tbl", d).Return(true, nil)

	lc := newLockCleaner(mockThemis, mockHbase)
	lc.eraseLockAndData([]byte("tbl"), []byte("row"), []hbase.Column{col}, 0)
}

func (s *LockCleanerTestSuit) TestCleanPrimaryLock(c *C) {
	mockHbase := s.buildMockHbase()
	mockThemis := s.buildMockThemis()
	lc := newLockCleaner(mockThemis, mockHbase)

	cc := generateTestColCoordinate()
	ts, _, err := lc.cleanPrimaryLock(cc, 1024)
	c.Assert(ts, Equals, uint64(2048))
	c.Assert(err, Equals, nil)
}
