package themis

import (
	"bytes"
	"encoding/binary"

	"github.com/ngaut/log"
	"github.com/pingcap/go-themis/hbase"
)

type ThemisScanner struct {
	scan *Scan
	txn  *Txn
	tbl  []byte
}

func newThemisScanner(tbl []byte, txn *Txn, c *client) *ThemisScanner {
	s := newScan(tbl, c)
	// add start ts
	b := bytes.NewBuffer(nil)
	binary.Write(b, binary.BigEndian, txn.startTs)
	s.addAttr("_themisTransationStartTs_", b.Bytes())
	return &ThemisScanner{
		scan: s,
		txn:  txn,
		tbl:  tbl,
	}
}

func (s *ThemisScanner) setStartRow(start []byte) {
	s.scan.StartRow = start
}

func (s *ThemisScanner) setStopRow(stop []byte) {
	s.scan.StopRow = stop
}

func (s *ThemisScanner) createGetFromScan(row []byte) *hbase.Get {
	g := hbase.CreateNewGet(row)
	for i, family := range s.scan.families {
		if len(s.scan.qualifiers[i]) > 0 {
			for _, qual := range s.scan.qualifiers[i] {
				g.AddColumn(family, qual)
			}
		} else {
			g.AddFamily(family)
		}
	}
	return g
}

func (s *ThemisScanner) Next() *hbase.ResultRow {
	r := s.scan.Next()
	if r == nil {
		return nil
	}
	// if we encounter conflict locks, we need to clean lock for this row and read again
	if isLockResult(r) {
		g := s.createGetFromScan(r.Row)
		r, err := s.txn.tryToCleanLockAndGetAgain(s.tbl, g, r.SortedColumns)
		if err != nil {
			log.Error(err)
			return nil
		}
		// empty result indicates the current row has been erased, we should get next row
		if len(r.SortedColumns) == 0 {
			return s.Next()
		} else {
			return r
		}
	}
	return r
}

func (s *ThemisScanner) Closed() bool {
	return s.scan.closed
}

func (s *ThemisScanner) Close() {
	if !s.scan.closed {
		s.scan.Close()
	}
}
