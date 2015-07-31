package themis

import (
	"errors"
	"strings"
)

func checkAndSetLockIsExpired(lock ThemisLock, client *themisClient, TTL uint64) (bool, error) {
	b, err := client.isLockExpired(lock.getColumn().table, lock.getColumn().row, lock.getTimestamp())
	if err != nil {
		return false, err
	}
	lock.setExpired(b)
	return b, nil
}

func getDataColFromMetaCol(lockOrWriteCol *column) (*column, error) {
	// get data column from lock column
	// key is like => L:family#qual, #p:family#qual
	parts := strings.Split(string(lockOrWriteCol.qual), "#")
	if len(parts) != 2 {
		return lockOrWriteCol, nil
	}
	c := &column{
		family: []byte(parts[0]),
		qual:   []byte(parts[1]),
	}
	return c, nil
}

func constructLocks(tbl []byte, lockKvs []*Kv, client *themisClient, TTL uint64) ([]ThemisLock, error) {
	var locks []ThemisLock
	for _, kv := range lockKvs {
		col := &columnCoordinate{
			table: tbl,
			row:   kv.row,
			column: column{
				family: kv.family,
				qual:   kv.qual,
			},
		}
		if !isLockColumn(&col.column) {
			return nil, errors.New("invalid lock")
		}
		l, err := parseLockFromBytes(kv.val)
		if err != nil {
			return nil, err
		}
		dataCol, _ := getDataColFromMetaCol(&col.column)
		cc := &columnCoordinate{
			table:  tbl,
			row:    kv.row,
			column: *dataCol,
		}
		l.setColumn(cc)
		checkAndSetLockIsExpired(l, client, TTL)
		locks = append(locks, l)
	}
	return locks, nil
}

func tryToCleanLock(tbl []byte, lockKvs []*Kv, cli *themisClient) error {
	return nil
}
