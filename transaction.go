package themis

import (
	"bytes"

	"github.com/ngaut/log"
	"github.com/pingcap/go-themis/oracle"
	"github.com/pingcap/go-themis/oracle/oracles"
)

type Txn struct {
	themisCli          *themisClient
	oracle             oracle.Oracle
	mutationCache      *columnMutationCache
	startTs            uint64
	primaryRow         *rowMutation
	primary            *columnCoordinate
	secondaryRows      []*rowMutation
	secondary          []*columnCoordinate
	primaryRowOffset   int
	singleRowTxn       bool
	secondaryLockBytes []byte
}

func NewTxn(c *Client) *Txn {
	txn := &Txn{
		themisCli:        &themisClient{c},
		mutationCache:    newColumnMutationCache(),
		oracle:           &oracles.LocalOracle{},
		primaryRowOffset: -1,
	}
	txn.startTs = txn.oracle.GetTimestamp()
	return txn
}

func isLockResult(r *ResultRow) bool {
	if len(r.SortedColumns) > 0 && isLockColumn(r.SortedColumns[0]) {
		return true
	}
	return false
}

func isLockColumn(c *ResultRowColumn) bool {
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

func tryCleanLock(table string, lockKvs *ResultRow) error {
	// TODO
	for _, c := range lockKvs.SortedColumns {
		if isLockColumn(c) {
			_, err := parseLockFromBytes([]byte(c.Value))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *Txn) Get(tbl string, g *ThemisGet) (*ResultRow, error) {
	return nil, nil
}

func (txn *Txn) Put(tbl string, p *ThemisPut) {
	for _, e := range p.put.Entries() {
		txn.mutationCache.addMutation([]byte(tbl), p.put.key, e.column, e.typ, e.value)
	}
}

func (txn *Txn) Commit() error {
	if txn.mutationCache.getSize() == 0 {
		return nil
	}

	txn.selectPrepareAndSecondary()
	txn.prewritePrimary()
	return nil
}

func (txn *Txn) selectPrepareAndSecondary() {
	txn.secondary = nil
	for tblName, rowMutations := range txn.mutationCache.mutations {
		for _, rowMutation := range rowMutations {
			row := rowMutation.row
			findPrimaryInRow := false
			for i, mutation := range rowMutation.mutationList() {
				colcord := newColumnCoordinate([]byte(tblName), row, mutation.family, mutation.qual)
				// set the first column as primary if primary is not set by user
				if txn.primaryRowOffset == -1 &&
					(txn.primary == nil || txn.primary.equal(colcord)) {
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
	secondaryLock := txn.constructSecondaryLock(TypePut)
	if secondaryLock != nil {
		txn.secondaryLockBytes = secondaryLock.toBytes()
	} else {
		txn.secondaryLockBytes = nil
	}
	log.Info(secondaryLock.primaryCoordinate)
	log.Info(txn.secondaryLockBytes)
}

func (txn *Txn) constructSecondaryLock(typ Type) *SecondaryLock {
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
	l.typ = txn.primaryRow.getType(txn.primary.column)
	l.ts = txn.startTs
	for _, c := range txn.secondary {
		l.addSecondaryColumn(c, txn.mutationCache.getMutation(c).typ)
	}
	return l
}

func (txn *Txn) prewriteRowWithLockClean(tbl []byte, mutation *rowMutation, containPrimary bool) {
	txn.prewriteRow(tbl, mutation, containPrimary)
}

func (txn *Txn) prewriteRow(tbl []byte, mutation *rowMutation, containPrimary bool) ThemisLock {
	if containPrimary {
		txn.themisCli.prewriteRow(tbl, mutation.row, mutation.mutationList(), txn.startTs, txn.constructPrimaryLock().toBytes(), txn.secondaryLockBytes, txn.primaryRowOffset)
	}
	return nil
}

func (txn *Txn) prewritePrimary() {
	txn.prewriteRowWithLockClean(txn.primary.table, txn.primaryRow, true)
}
