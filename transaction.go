package themis

import (
	"bytes"

	"github.com/ngaut/log"
	"github.com/pingcap/go-themis/hbase"
	"github.com/pingcap/go-themis/oracle"
	"github.com/pingcap/go-themis/oracle/oracles"
)

type Txn struct {
	themisCli          themisClient
	oracle             oracle.Oracle
	mutationCache      *columnMutationCache
	startTs            uint64
	commitTs           uint64
	primaryRow         *rowMutation
	primary            *hbase.ColumnCoordinate
	secondaryRows      []*rowMutation
	secondary          []*hbase.ColumnCoordinate
	primaryRowOffset   int
	lockCleaner        *lockCleaner
	singleRowTxn       bool
	secondaryLockBytes []byte
}

func NewTxn(c *client) *Txn {
	txn := &Txn{
		themisCli:        newThemisClient(c),
		mutationCache:    newColumnMutationCache(),
		oracle:           &oracles.LocalOracle{},
		primaryRowOffset: -1,
	}
	txn.startTs = txn.oracle.GetTimestamp()
	txn.lockCleaner = newLockCleaner(txn.themisCli, c)
	return txn
}

func isLockResult(r *hbase.ResultRow) bool {
	col := &hbase.Column{
		Family: r.SortedColumns[0].Family,
		Qual:   r.SortedColumns[0].Qual,
	}
	if len(r.SortedColumns) > 0 && isLockColumn(col) {
		return true
	}
	return false
}

func isLockColumn(c *hbase.Column) bool {
	if bytes.Compare(c.Family, LockFamilyName) == 0 {
		return true
	}
	return false
}

func shouldClean(l ThemisLock) bool {
	// TODO check worker alive
	return l.isExpired()
}

func cleanLock(l ThemisLock) {
}

func (t *Txn) Get(tbl string, g *hbase.Get) (*hbase.ResultRow, error) {
	return nil, nil
}

func (txn *Txn) Put(tbl string, p *hbase.Put) {
	for _, e := range getEntriesFromPut(p) {
		txn.mutationCache.addMutation([]byte(tbl), p.Row, e.Column, e.typ, e.value)
	}
}

func (txn *Txn) Commit() error {
	if txn.mutationCache.getSize() == 0 {
		return nil
	}

	txn.selectPrepareAndSecondary()
	err := txn.prewritePrimary()
	if err != nil {
		return err
	}
	err = txn.prewriteSecondary()
	if err != nil {
		return err
	}

	txn.commitTs = txn.oracle.GetTimestamp()
	err = txn.commitPrimary()
	if err != nil {
		// TODO rollback
		log.Fatal(err)
	}
	txn.commitSecondary()
	return nil
}

func (txn *Txn) commitSecondary() {
	for _, r := range txn.secondaryRows {
		err := txn.themisCli.commitSecondaryRow(r.tbl, r.row, r.mutationList(false), txn.startTs, txn.commitTs)
		if err != nil {
			// fail of secondary commit will not stop the commits of next
			// secondaries
			log.Warning(err)
		}
	}
}

func (txn *Txn) commitPrimary() error {
	return txn.themisCli.commitRow(txn.primary.Table, txn.primary.Row,
		txn.primaryRow.mutationList(false),
		txn.startTs, txn.commitTs, txn.primaryRowOffset)
}

func (txn *Txn) prewriteSecondary() error {
	for _, rowMutation := range txn.secondaryRows {
		_, err := txn.prewriteRow(rowMutation.tbl, rowMutation, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func (txn *Txn) selectPrepareAndSecondary() {
	txn.secondary = nil
	for tblName, rowMutations := range txn.mutationCache.mutations {
		for _, rowMutation := range rowMutations {
			row := rowMutation.row
			findPrimaryInRow := false
			for i, mutation := range rowMutation.mutationList(true) {
				colcord := hbase.NewColumnCoordinate([]byte(tblName), row, mutation.Family, mutation.Qual)
				// set the first column as primary if primary is not set by user
				if txn.primaryRowOffset == -1 &&
					(txn.primary == nil || txn.primary.Equal(colcord)) {
					txn.primary = colcord
					txn.primaryRowOffset = i
					txn.primaryRow = rowMutation
					findPrimaryInRow = true
				} else {
					txn.secondary = append(txn.secondary, colcord)
				}
			}
			if !findPrimaryInRow {
				txn.secondaryRows = append(txn.secondaryRows, rowMutation)
			}
		}
	}
	if len(txn.secondaryRows) == 0 {
		txn.singleRowTxn = true
	}
	// construct secondary lock
	secondaryLock := txn.constructSecondaryLock(hbase.TypePut)
	if secondaryLock != nil {
		txn.secondaryLockBytes = secondaryLock.toBytes()
	} else {
		txn.secondaryLockBytes = nil
	}
	log.Info(secondaryLock.primaryCoordinate)
	log.Info(txn.secondaryLockBytes)
}

func (txn *Txn) constructSecondaryLock(typ hbase.Type) *SecondaryLock {
	if txn.primaryRow.getSize() <= 1 && len(txn.secondaryRows) == 0 {
		return nil
	}
	l := newSecondaryLock()
	l.primaryCoordinate = txn.primary
	l.ts = txn.startTs
	// TODO set client addr
	return l
}

func (txn *Txn) constructPrimaryLock() *PrimaryLock {
	l := newPrimaryLock()
	l.typ = txn.primaryRow.getType(txn.primary.Column)
	l.ts = txn.startTs
	for _, c := range txn.secondary {
		l.addSecondaryColumn(c, txn.mutationCache.getMutation(c).typ)
	}
	return l
}

func (txn *Txn) prewriteRowWithLockClean(tbl []byte, mutation *rowMutation, containPrimary bool) error {
	_, err := txn.prewriteRow(tbl, mutation, containPrimary)
	if err != nil {
		return err
	}
	return nil
}

func (txn *Txn) prewriteRow(tbl []byte, mutation *rowMutation, containPrimary bool) (ThemisLock, error) {
	if containPrimary {
		// try to get lock
		lock, err := txn.themisCli.prewriteRow(tbl, mutation.row,
			mutation.mutationList(true),
			txn.startTs,
			txn.constructPrimaryLock().toBytes(),
			txn.secondaryLockBytes, txn.primaryRowOffset)

		if err != nil {
			return nil, err
		}

		// some other got the lock, try to clean it
		if lock != nil {
			expired, err := txn.themisCli.checkAndSetLockIsExpired(lock, 0)
			if err != nil {
				return nil, err
			}
			if expired {
				// try to clean primary lock
				log.Info("lock expired, try clean primary lock")
				pl := lock.getPrimaryLock()
				commitTs, cleanedLock, err := txn.lockCleaner.cleanPrimaryLock(pl.getColumn(), pl.getTimestamp())
				if err != nil {
					return nil, err
				}
				//
				if cleanedLock != nil {
					pl = cleanedLock
				}
				log.Info("try clean secondary locks")
				// clean secondary locks
				// erase lock and data if commitTs is 0; otherwise, commit it.
				for k, v := range pl.(*PrimaryLock).secondaries {
					log.Info(k, v)
					cc := hbase.ColumnCoordinate{}
					cc.ParseFromString(k)
					if commitTs == 0 {
						// expire trx havn't committed yet, we must delete lock and
						// dirty data
						err = txn.lockCleaner.eraseLockAndData(cc.Table, cc.Row, cc.Column, pl.getTimestamp())
						if err != nil {
							return nil, err
						}
					} else {
						// primary row is committed, so we must commit other
						// secondary rows
						mutation := &columnMutation{
							Column: &cc.Column,
							mutationValuePair: &mutationValuePair{
								typ: v,
							},
						}
						err = txn.themisCli.commitSecondaryRow(cc.Table, cc.Row,
							[]*columnMutation{mutation}, pl.getTimestamp(), commitTs)
						if err != nil {
							return nil, err
						}
					}
				}
				// TODO try prewrite row again
				//log.Info("try prewrite row again")
				//return txn.prewriteRow(tbl, mutation, containPrimary)
			}
		} else {
			// get lock successful
			log.Info("got the lock")
		}
	} else {
		return txn.themisCli.prewriteSecondaryRow(tbl, mutation.row, mutation.mutationList(true), txn.startTs, txn.secondaryLockBytes)
	}
	return nil, nil
}

func (txn *Txn) prewritePrimary() error {
	err := txn.prewriteRowWithLockClean(txn.primary.Table, txn.primaryRow, true)
	if err != nil {
		return err
	}
	return nil
}
