package themis

import (
	"errors"
	"strings"

	"github.com/pingcap/go-themis/hbase"
)

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

func constructLocks(tbl []byte, lockKvs []*hbase.Kv, client *themisClient, TTL uint64) ([]ThemisLock, error) {
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

func tryToCleanLock(tbl []byte, lockKvs []*hbase.Kv, cli *themisClient) error {
	return nil
}
