package themis

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"strings"

	"github.com/pingcap/go-themis/hbase"
)

type lockCleaner interface {
	cleanPrimaryLock(cc *hbase.ColumnCoordinate, prewriteTs uint64) (uint64, ThemisLock, error)
	eraseLockAndData(tbl []byte, row []byte, col hbase.Column, ts uint64) error
}

var _ lockCleaner = (*lockCleanerImpl)(nil)

type lockCleanerImpl struct {
	themisCli themisClient
	hbaseCli  hbaseClient
}

func newLockCleaner(cli themisClient, hbaseCli hbaseClient) lockCleaner {
	return &lockCleanerImpl{cli, hbaseCli}
}

func getDataColFromMetaCol(lockOrWriteCol hbase.Column) hbase.Column {
	// get data column from lock column
	// key is like => L:family#qual, #p:family#qual
	parts := strings.Split(string(lockOrWriteCol.Qual), "#")
	if len(parts) != 2 {
		return lockOrWriteCol
	}
	c := hbase.Column{
		Family: []byte(parts[0]),
		Qual:   []byte(parts[1]),
	}
	return c
}

func constructLocks(tbl []byte, lockKvs []*hbase.Kv, client themisClient, TTL uint64) ([]ThemisLock, error) {
	var locks []ThemisLock
	for _, kv := range lockKvs {
		col := &hbase.ColumnCoordinate{
			Table: tbl,
			Row:   kv.Row,
			Column: hbase.Column{
				Family: kv.Family,
				Qual:   kv.Qual,
			},
		}
		if !isLockColumn(&col.Column) {
			return nil, errors.New("invalid lock")
		}
		l, err := parseLockFromBytes(kv.Value)
		if err != nil {
			return nil, err
		}
		cc := &hbase.ColumnCoordinate{
			Table:  tbl,
			Row:    kv.Row,
			Column: getDataColFromMetaCol(col.Column),
		}
		l.setColumn(cc)
		client.checkAndSetLockIsExpired(l, TTL)
		locks = append(locks, l)
	}
	return locks, nil
}

func (cleaner *lockCleanerImpl) cleanPrimaryLock(cc *hbase.ColumnCoordinate, prewriteTs uint64) (uint64, ThemisLock, error) {
	l, err := cleaner.themisCli.getLockAndErase(cc, prewriteTs)
	if err != nil {
		return 0, nil, err
	}
	pl, _ := l.(*PrimaryLock)
	// if primary lock is nil, means someothers have already committed
	if pl == nil {
		g := hbase.CreateNewGet(cc.Row)
		// add put write column
		qual := string(cc.Family) + "#" + string(cc.Qual)
		g.AddStringColumn("#p", qual)
		// add del write column
		g.AddStringColumn("#d", qual)
		g.AddTimeRange(prewriteTs, math.MaxUint64)
		r, err := cleaner.hbaseCli.Get(string(cc.Table), g)
		if err != nil {
			return 0, nil, err
		}
		for _, kv := range r.SortedColumns {
			var ts uint64
			binary.Read(bytes.NewBuffer(kv.Value), binary.BigEndian, &ts)
			if ts == prewriteTs {
				// get this commit's commitTs
				return kv.Ts, nil, nil
			}
		}
	} else {
		return 0, pl, nil
	}
	panic("should not be here")
	return 0, nil, nil
}

func (cleaner *lockCleanerImpl) eraseLockAndData(tbl []byte, row []byte, col hbase.Column, ts uint64) error {
	d := hbase.CreateNewDelete(row)
	// delete lock
	d.AddColumnWithTimestamp(LockFamilyName, []byte(string(col.Family)+"#"+string(col.Qual)), ts)
	// delete dirty val
	d.AddColumnWithTimestamp(col.Family, col.Qual, ts)
	_, err := cleaner.hbaseCli.Delete(string(tbl), d)
	return err
}
