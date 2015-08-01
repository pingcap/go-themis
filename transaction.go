package themis

import (
	"bytes"

	"github.com/ngaut/log"
	"github.com/pingcap/go-themis/hbase"
	"github.com/pingcap/go-themis/oracle"
	"github.com/pingcap/go-themis/oracle/oracles"
)

type Txn struct {
	themisCli          *themisClient
	oracle             oracle.Oracle
	mutationCache      *columnMutationCache
	startTs            uint64
	primaryRow         *rowMutation
	primary            *hbase.ColumnCoordinate
	secondaryRows      []*rowMutation
	secondary          []*hbase.ColumnCoordinate
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

func tryCleanLock(table string, lockKvs *hbase.ResultRow) error {
	// TODO
	for _, c := range lockKvs.SortedColumns {
		if isLockColumn(&hbase.Column{c.Family, c.Qual}) {
			_, err := parseLockFromBytes([]byte(c.Value))
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *Txn) Get(tbl string, g *hbase.Get) (*hbase.ResultRow, error) {
	return nil, nil
}

func (txn *Txn) Put(tbl string, p *hbase.Put) {
	//for _, e := range p.Entries() {
	//	txn.mutationCache.addMutation([]byte(tbl), p.put.key, e.column, e.typ, e.value)
	//}
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
	return nil
}

func (txn *Txn) selectPrepareAndSecondary() {
	txn.secondary = nil
	for tblName, rowMutations := range txn.mutationCache.mutations {
		for _, rowMutation := range rowMutations {
			row := rowMutation.row
			findPrimaryInRow := false
			for i, mutation := range rowMutation.mutationList() {
				colcord := hbase.NewColumnCoordinate([]byte(tblName), row, mutation.Family, mutation.Qual)
				// set the first column as primary if primary is not set by user
				if txn.primaryRowOffset == -1 &&
					(txn.primary == nil || txn.primary.Equal(colcord)) {
					txn.primary = colcord
					txn.primaryRowOffset = i
					txn.primaryRow = rowMutation
					findPrimaryInRow = true
					log.Warning(i, string(txn.primaryRow.row))
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
		_, err := txn.themisCli.prewriteRow(tbl, mutation.row, mutation.mutationList(), txn.startTs, txn.constructPrimaryLock().toBytes(), txn.secondaryLockBytes, txn.primaryRowOffset)
		if err != nil {
			return nil, err
		}
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
