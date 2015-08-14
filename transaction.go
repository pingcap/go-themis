package themis

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/c4pt0r/go-hbase"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/go-themis/oracle"
	"github.com/pingcap/go-themis/oracle/oracles"
)

type TxnConfig struct {
	ConcurrentPrewriteAndCommit bool
	brokenCommitTest            bool
	brokenPrewriteSecondaryTest bool
}

var defaultTxnConf = TxnConfig{
	ConcurrentPrewriteAndCommit: true,
	brokenCommitTest:            false,
	brokenPrewriteSecondaryTest: false,
}

type Txn struct {
	client             hbase.HBaseClient
	themisCli          themisClient
	lockCleaner        lockCleaner
	oracle             oracle.Oracle
	mutationCache      *columnMutationCache
	startTs            uint64
	commitTs           uint64
	primaryRow         *rowMutation
	primary            *hbase.ColumnCoordinate
	secondaryRows      []*rowMutation
	secondary          []*hbase.ColumnCoordinate
	primaryRowOffset   int
	singleRowTxn       bool
	secondaryLockBytes []byte
	conf               TxnConfig
}

var localOracle = &oracles.LocalOracle{}

func NewTxn(c hbase.HBaseClient) *Txn {
	txn := &Txn{
		client:           c,
		themisCli:        newThemisClient(c),
		mutationCache:    newColumnMutationCache(),
		oracle:           localOracle,
		primaryRowOffset: -1,
		conf:             defaultTxnConf,
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

func (txn *Txn) AddConfig(conf TxnConfig) *Txn {
	txn.conf = conf
	return txn
}

func (txn *Txn) Get(tbl string, g *hbase.Get) (*hbase.ResultRow, error) {
	r, err := txn.themisCli.themisGet([]byte(tbl), g, txn.startTs, false)
	if err != nil {
		return nil, err
	}
	// contain locks, try to clean and get again
	if r != nil && isLockResult(r) {
		log.Warning("get lock, try to clean and get again")
		r, err = txn.tryToCleanLockAndGetAgain([]byte(tbl), g, r.SortedColumns)
		if err != nil {
			return nil, err
		}
	}
	return r, nil
}

func (txn *Txn) Put(tbl string, p *hbase.Put) {
	// add mutation to buffer
	for _, e := range getEntriesFromPut(p) {
		txn.mutationCache.addMutation([]byte(tbl), p.Row, e.Column, e.typ, e.value)
	}
}

func (txn *Txn) Delete(tbl string, p *hbase.Delete) {
	for _, e := range getEntriesFromDel(p) {
		txn.mutationCache.addMutation([]byte(tbl), p.Row, e.Column, e.typ, e.value)
	}
}

func (txn *Txn) Commit() error {
	if txn.mutationCache.getSize() == 0 {
		// read-only transaction
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
		// commit primary error, rollback
		txn.rollbackRow(txn.primaryRow.tbl, txn.primaryRow)
		txn.rollbackSecondaryRow(len(txn.secondaryRows) - 1)
		return err
	}
	txn.commitSecondary()
	return nil
}

func (txn *Txn) commitSecondary() {
	if txn.conf.brokenCommitTest {
		txn.brokenCommitSecondary()
		return
	}
	if txn.conf.ConcurrentPrewriteAndCommit {
		txn.commitSecondaryConcurrent()
	} else {
		txn.commitSecondarySync()
	}
}

func (txn *Txn) commitSecondarySync() {
	log.Info("commit secondary sync")
	for _, r := range txn.secondaryRows {
		err := txn.themisCli.commitSecondaryRow(r.tbl, r.row, r.mutationList(false), txn.startTs, txn.commitTs)
		if err != nil {
			// fail of secondary commit will not stop the commits of next
			// secondaries
			log.Warning(err)
		}
	}
}

func (txn *Txn) commitSecondaryConcurrent() {
	log.Info("commit secondary concurrent")
	wg := sync.WaitGroup{}
	for _, r := range txn.secondaryRows {
		wg.Add(1)
		go func(r *rowMutation) {
			defer wg.Done()
			err := txn.themisCli.commitSecondaryRow(r.tbl, r.row, r.mutationList(false), txn.startTs, txn.commitTs)
			if err != nil {
				// fail of secondary commit will not stop the commits of next
				// secondaries
				log.Warning(err)
			}
		}(r)
	}
	wg.Wait()
}

func (txn *Txn) commitPrimary() error {
	return txn.themisCli.commitRow(txn.primary.Table, txn.primary.Row,
		txn.primaryRow.mutationList(false),
		txn.startTs, txn.commitTs, txn.primaryRowOffset)
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

func (txn *Txn) tryToCleanLockAndGetAgain(tbl []byte, g *hbase.Get, lockKvs []*hbase.Kv) (*hbase.ResultRow, error) {
	// try to clean locks
	locks, err := constructLocks([]byte(tbl), lockKvs, txn.themisCli)
	for _, lock := range locks {
		err := txn.tryToCleanLock(lock)
		if err != nil {
			return nil, err
		}
	}
	// get again, ignore lock
	r, err := txn.themisCli.themisGet([]byte(tbl), g, txn.startTs, true)
	if err != nil {
		return nil, err
	}
	return r, nil
}

func (txn *Txn) tryToCleanLock(lock ThemisLock) error {
	log.Warn("try to clean lock")
	expired, err := txn.themisCli.checkAndSetLockIsExpired(lock)
	if err != nil {
		return err
	}
	// only clean expired lock
	if expired {
		// try to clean primary lock
		log.Info("lock expired, try clean primary lock")
		pl := lock.getPrimaryLock()
		commitTs, cleanedLock, err := txn.lockCleaner.cleanPrimaryLock(pl.getColumn(), pl.getTimestamp())
		if err != nil {
			return err
		}
		//
		if cleanedLock != nil {
			pl = cleanedLock
		}
		log.Info("try clean secondary locks")
		// clean secondary locks
		// erase lock and data if commitTs is 0; otherwise, commit it.
		for k, v := range pl.(*PrimaryLock).secondaries {
			cc := hbase.ColumnCoordinate{}
			cc.ParseFromString(k)
			if commitTs == 0 {
				// commitTs == 0, means clean primary lock successfully
				// expire trx havn't committed yet, we must delete lock and
				// dirty data
				err = txn.lockCleaner.eraseLockAndData(cc.Table, cc.Row, []hbase.Column{cc.Column}, pl.getTimestamp())
				if err != nil {
					return err
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
					return err
				}
			}
		}
	} else {
		log.Warn("lock is not expired")
	}
	return nil
}

func (txn *Txn) prewriteRowWithLockClean(tbl []byte, mutation *rowMutation, containPrimary bool) error {
	lock, err := txn.prewriteRow(tbl, mutation, containPrimary)
	if err != nil {
		return err
	}
	// lock clean
	if lock != nil {
		err = txn.tryToCleanLock(lock)
		if err != nil {
			return err
		}
		// try one more time after clean lock successfully
		lock, err = txn.prewriteRow(tbl, mutation, containPrimary)
		if err != nil {
			return err
		}
		if lock != nil {
			return fmt.Errorf("can't clean lock, column:%+v; conflict lock: %+v, lock ts: %d", lock.getColumn(), lock, lock.getTimestamp())
		}
	}
	return nil
}

func (txn *Txn) prewriteRow(tbl []byte, mutation *rowMutation, containPrimary bool) (ThemisLock, error) {
	if containPrimary {
		// try to get lock
		return txn.themisCli.prewriteRow(tbl, mutation.row,
			mutation.mutationList(true),
			txn.startTs,
			txn.constructPrimaryLock().toBytes(),
			txn.secondaryLockBytes, txn.primaryRowOffset)
	} else {
		return txn.themisCli.prewriteSecondaryRow(tbl, mutation.row,
			mutation.mutationList(true),
			txn.startTs,
			txn.secondaryLockBytes)
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

func (txn *Txn) prewriteSecondary() error {
	if txn.conf.brokenPrewriteSecondaryTest {
		return txn.brokenPrewriteSecondary()
	}
	if txn.conf.ConcurrentPrewriteAndCommit {
		return txn.prewriteSecondaryConcurrent()
	}
	return txn.prewriteSecondarySync()
}

func (txn *Txn) prewriteSecondarySync() error {
	for i, mu := range txn.secondaryRows {
		err := txn.prewriteRowWithLockClean(mu.tbl, mu, false)
		if err != nil {
			// rollback
			txn.rollbackRow(txn.primaryRow.tbl, txn.primaryRow)
			txn.rollbackSecondaryRow(i)
			return err
		}
	}
	return nil
}

// just for test
func (txn *Txn) brokenCommitSecondary() {
	// do nothing
	log.Warn("Simulating secondary commit failed")
}

func (txn *Txn) brokenPrewriteSecondary() error {
	log.Warn("Simulating prewrite secondary failed")
	for i, rm := range txn.secondaryRows {
		if i == len(txn.secondary)-1 {
			// simulating prewrite failed, need rollback
			txn.rollbackRow(txn.primaryRow.tbl, txn.primaryRow)
			txn.rollbackSecondaryRow(i)
			return errors.New("simulated error")
		}
		txn.prewriteRowWithLockClean(rm.tbl, rm, false)
	}
	return nil
}

func (txn *Txn) prewriteSecondaryConcurrent() error {
	wg := sync.WaitGroup{}

	errChan := make(chan error, len(txn.secondaryRows))
	defer close(errChan)
	successChan := make(chan *rowMutation, len(txn.secondaryRows))
	defer close(successChan)

	for i, rm := range txn.secondaryRows {
		wg.Add(1)
		go func(i int, mutation *rowMutation) {
			defer wg.Done()
			err := txn.prewriteRowWithLockClean(mutation.tbl, mutation, false)
			if err != nil {
				// need rollback
				errChan <- err
			} else {
				successChan <- mutation
			}
		}(i, rm)
	}
	wg.Wait()

	if len(errChan) != 0 {
		// occur error, clean success prewrite mutations
		log.Warning("prewrite secondary rows encounter error, rolling back", len(successChan))
		txn.rollbackRow(txn.primaryRow.tbl, txn.primaryRow)
	L:
		for {
			select {
			case succMutation := <-successChan:
				{
					txn.rollbackRow(succMutation.tbl, succMutation)
				}
			default:
				break L
			}
		}
	}
	return nil
}

func (txn *Txn) rollbackRow(tbl []byte, mutation *rowMutation) error {
	l := fmt.Sprintf("\nrolling back %s {\n", string(tbl))
	for _, v := range mutation.getColumns() {
		l += fmt.Sprintf("\t%s:%s\n", string(v.Family), string(v.Qual))
	}
	l += "}\n"
	log.Warn(l)
	return txn.lockCleaner.eraseLockAndData(tbl, mutation.row, mutation.getColumns(), txn.startTs)
}

func (txn *Txn) rollbackSecondaryRow(successIndex int) error {
	for i := successIndex; i >= 0; i-- {
		r := txn.secondaryRows[i]
		err := txn.rollbackRow(r.tbl, r)
		if err != nil {
			return err
		}
	}
	return nil
}

func (txn *Txn) GetScanner(tbl []byte, startKey, endKey []byte) *ThemisScanner {
	scanner := newThemisScanner(tbl, txn, txn.client)
	if startKey != nil {
		scanner.setStartRow(startKey)
	}
	if endKey != nil {
		scanner.setStopRow(endKey)
	}
	return scanner
}

func (txn *Txn) Release() {
	txn.primary = nil
	txn.primaryRow = nil
	txn.secondary = nil
	txn.secondaryRows = nil
	txn.startTs = 0
	txn.commitTs = 0
}

func (txn *Txn) String() string {
	return fmt.Sprintf("%d", txn.startTs)
}
