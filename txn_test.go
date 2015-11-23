package themis

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/juju/errors"

	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	"github.com/pingcap/go-hbase"
)

type TransactionTestSuit struct {
	cli hbase.HBaseClient
}

var _ = Suite(&TransactionTestSuit{})

func Test(t *testing.T) { TestingT(t) }

func (s *TransactionTestSuit) SetUpSuite(c *C) {
	var err error
	s.cli, err = createHBaseClient()
	c.Assert(err, Equals, nil)
}

func (s *TransactionTestSuit) SetUpTest(c *C) {
	log.Warn("new test, reset tables")
	err := createNewTableAndDropOldTable(s.cli, themisTestTableName, string(cf), nil)
	c.Assert(err, IsNil)
}

func (s *TransactionTestSuit) TestAsyncCommit(c *C) {
	p := hbase.NewPut(testRow).AddValue(cf, q, []byte("val"))
	tx := newTxn(s.cli)
	tx.Put(themisTestTableName, p)
	tx.Commit()

	tx = newTxn(s.cli)
	d := hbase.NewDelete([]byte("test")).AddColumn(cf, q)
	tx.Delete(themisTestTableName, d)
	tx.Commit()

	tx = newTxn(s.cli)
	g := hbase.NewGet([]byte("test")).AddFamily(cf)
	r, err := tx.Get(themisTestTableName, g)
	c.Assert(err, Equals, nil)
	c.Assert(r, IsNil)
	tx.Commit()

	conf := defaultTxnConf
	conf.brokenCommitSecondaryTest = true

	tx = newTxn(s.cli).AddConfig(conf)
	// simulating broken commit
	for i := 0; i < 10; i++ {
		p := hbase.NewPut([]byte(fmt.Sprintf("test_%d", i)))
		p.AddValue(cf, q, []byte(fmt.Sprintf("%d", tx.(*themisTxn).GetStartTS())))
		tx.Put(themisTestTableName, p)
	}
	err = tx.Commit()
	c.Assert(err, Equals, nil)

	//  wait until lock expired.
	log.Warn("Wait for lock expired. Sleep...")
	tick := 6
	for tick > 0 {
		time.Sleep(1 * time.Second)
		tick--
		log.Infof("remain %ds...", tick)
	}

	log.Warn("Try commit again")
	// new transction will not see lock
	tx = newTxn(s.cli)
	for i := 0; i < 5; i++ {
		p := hbase.NewPut([]byte(fmt.Sprintf("test_%d", i)))
		p.AddValue(cf, q, []byte(fmt.Sprintf("%d", tx.(*themisTxn).GetStartTS())))
		tx.Put(themisTestTableName, p)
	}
	err = tx.Commit()
	if err != nil {
		log.Error(err)
	}
	c.Assert(err, Equals, nil)

	tx = newTxn(s.cli)
	for i := 5; i < 10; i++ {
		p := hbase.NewPut([]byte(fmt.Sprintf("test_%d", i)))
		p.AddValue(cf, q, []byte(fmt.Sprintf("%d", tx.(*themisTxn).GetStartTS())))
		tx.Put(themisTestTableName, p)
	}
	err = tx.Commit()
	if err != nil {
		log.Error(errors.ErrorStack(err))
	}
	c.Assert(err, Equals, nil)

}

func (s *TransactionTestSuit) TestBrokenPrewriteSecondary(c *C) {
	tx := newTxn(s.cli)
	ts := tx.(*themisTxn).GetStartTS()
	// simulating broken commit
	for i := 0; i < 10; i++ {
		p := hbase.NewPut([]byte(fmt.Sprintf("test_%d", i)))
		p.AddValue(cf, q, []byte(fmt.Sprintf("%d", ts)))
		tx.Put(themisTestTableName, p)
	}
	err := tx.Commit()
	c.Assert(err, IsNil)

	// TODO: check rallback & cleanup locks
	conf := defaultTxnConf
	conf.brokenPrewriteSecondaryTest = true

	tx = newTxn(s.cli).AddConfig(conf)
	ts = tx.GetStartTS()
	// simulating broken commit
	for i := 0; i < 10; i++ {
		p := hbase.NewPut([]byte(fmt.Sprintf("test_%d", i)))
		p.AddValue(cf, q, []byte(fmt.Sprintf("%d", ts)))
		tx.Put(themisTestTableName, p)
	}
	err = tx.Commit()
	c.Assert(err, NotNil)

	// check if locks are cleaned successfully
	tx = newTxn(s.cli)
	for i := 0; i < 10; i++ {
		g := hbase.NewGet([]byte(fmt.Sprintf("test_%d", i)))
		r, err := tx.Get(themisTestTableName, g)
		c.Assert(err, Equals, nil)
		c.Assert(r == nil || string(r.SortedColumns[0].Value) != fmt.Sprintf("%d", ts), Equals, true)
	}
}

func (s *TransactionTestSuit) TestPrimaryLockTimeout(c *C) {
	// TODO: check if lock can be cleaned up when secondary prewrite failed and
	// rollback is also failed
	conf := defaultTxnConf
	conf.brokenPrewriteSecondaryTest = true
	conf.brokenPrewriteSecondaryAndRollbackTest = true
	tx := newTxn(s.cli).AddConfig(conf)
	ts := tx.GetStartTS()
	// simulating broken commit
	for i := 0; i < 2; i++ {
		p := hbase.NewPut([]byte(fmt.Sprintf("test_%d", i)))
		p.AddValue(cf, q, []byte(fmt.Sprintf("%d", ts)))
		tx.Put(themisTestTableName, p)
	}
	err := tx.Commit()
	c.Assert(err, NotNil)
	log.Error(err)

	//  wait until lock expired.
	log.Warn("Wait for lock expired. Sleep...")
	tick := 6
	for tick > 0 {
		time.Sleep(1 * time.Second)
		tick--
		log.Infof("remain %ds...", tick)
	}

	// check if locks are cleaned successfully
	tx = newTxn(s.cli)
	for i := 0; i < 2; i++ {
		g := hbase.NewGet([]byte(fmt.Sprintf("test_%d", i)))
		r, err := tx.Get(themisTestTableName, g)
		c.Assert(err, Equals, nil)
		// this commit must rollback
		c.Assert(r == nil || string(r.SortedColumns[0].Value) != fmt.Sprintf("%d", ts), Equals, true)
		log.Info(r, err)
	}
}

func checkCommitSuccess(s *TransactionTestSuit, c *C, row []byte) {
	tx := newTxn(s.cli)
	colMap := make(map[string]string)
	colMap["#p:"+string(cf)+"#q"] = ""
	colMap[string(cf)+":q"] = ""
	r, err := tx.(*themisTxn).client.Get(themisTestTableName, hbase.NewGet(row))
	c.Assert(err, Equals, nil)
	c.Assert(2, Equals, len(r.Columns))
	for _, v := range r.Columns {
		_, exist := colMap[string(v.Family)+":"+string(v.Qual)]
		c.Assert(exist, Equals, true)
	}
}

func (s *TransactionTestSuit) TestLockRow(c *C) {
	tx := newTxn(s.cli)
	row := []byte("lockRow")
	put := hbase.NewPut(row)
	put.AddValue(cf, q, []byte("v"))
	tx.Put(themisTestTableName, put)
	tx.Commit()

	checkCommitSuccess(s, c, row)

	tx = newTxn(s.cli)
	err := tx.LockRow(themisTestTableName, row)
	c.Assert(err, Equals, nil)

	tx.(*themisTxn).selectPrimaryAndSecondaries()
	err = tx.(*themisTxn).prewritePrimary()
	c.Assert(err, Equals, nil)
	colMap := make(map[string]string)
	colMap["#p:"+string(cf)+"#q"] = ""
	colMap[string(cf)+":q"] = ""
	colMap["L:"+string(cf)+"#q"] = ""
	var r *hbase.ResultRow
	r, err = tx.(*themisTxn).client.Get(themisTestTableName, hbase.NewGet(row))
	c.Assert(err, Equals, nil)
	c.Assert(3, Equals, len(r.Columns))
	for _, v := range r.Columns {
		_, exist := colMap[string(v.Family)+":"+string(v.Qual)]
		c.Assert(exist, Equals, true)
	}
	tx.(*themisTxn).commitTs = tx.GetStartTS() + 1
	tx.(*themisTxn).commitPrimary()
	checkCommitSuccess(s, c, row)
}

func isWrongRegionErr(err error) bool {
	return strings.Contains(err.Error(), "org.apache.hadoop.hbase.regionserver.WrongRegionException")
}

func (s *TransactionTestSuit) TestBatchGet(c *C) {
	batchGetTestTbl := "batch_get_test"
	err := createNewTableAndDropOldTable(s.cli, batchGetTestTbl, string(cf), [][]byte{
		// split in middle
		[]byte("batch_test_5"),
	})
	defer dropTable(s.cli, batchGetTestTbl)
	// prepare data
	tx := newTxn(s.cli)
	for i := 0; i < 10; i++ {
		p := hbase.NewPut([]byte(fmt.Sprintf("batch_test_%d", i))).AddValue(cf, q, []byte("v"))
		tx.Put(batchGetTestTbl, p)
	}
	err = tx.Commit()
	c.Assert(err, IsNil)

	// batch get
	var gets []*hbase.Get
	for i := 0; i < 10; i++ {
		g := hbase.NewGet([]byte(fmt.Sprintf("batch_test_%d", i))).AddColumn(cf, q)
		gets = append(gets, g)
	}
	for i := 5; i < 10; i++ {
		g := hbase.NewGet([]byte(fmt.Sprintf("batch_test_no_such_row_%d", i))).AddColumn(cf, q)
		gets = append(gets, g)
	}
	tx = newTxn(s.cli)
	_, err = tx.Gets(batchGetTestTbl, gets)
	c.Assert(isWrongRegionErr(err), Equals, true)

	gets = nil
	for i := 0; i < 5; i++ {
		g := hbase.NewGet([]byte(fmt.Sprintf("batch_test_%d", i))).AddColumn(cf, q)
		gets = append(gets, g)
	}
	tx = newTxn(s.cli)
	rs, err := tx.Gets(batchGetTestTbl, gets)
	c.Assert(err, IsNil)
	c.Assert(len(rs), Equals, 5)
}

func (s *TransactionTestSuit) TestBatchGetWithLocks(c *C) {
	// simulating locks
	conf := defaultTxnConf
	conf.brokenCommitSecondaryTest = true

	tx := newTxn(s.cli).AddConfig(conf)
	ts := tx.GetStartTS()
	// simulating broken commit
	for i := 0; i < 10; i++ {
		p := hbase.NewPut([]byte(fmt.Sprintf("batch_test_with_lock_%d", i)))
		p.AddValue(cf, q, []byte(fmt.Sprintf("%d", ts)))
		tx.Put(themisTestTableName, p)
	}
	tx.Commit()

	tx = newTxn(s.cli)

	var gets []*hbase.Get
	for i := 0; i < 10; i++ {
		g := hbase.NewGet([]byte(fmt.Sprintf("batch_test_with_lock_%d", i))).AddColumn(cf, q)
		gets = append(gets, g)
	}
	rs, err := tx.Gets(themisTestTableName, gets)
	c.Assert(err, IsNil)
	// we had already cleaned secondary locks
	c.Assert(len(rs), Equals, 10)
}

func (s *TransactionTestSuit) TestAsyncSecondaryCommit(c *C) {
	conf := defaultTxnConf
	conf.brokenCommitSecondaryTest = true
	tx := newTxn(s.cli).AddConfig(conf)
	for i := 0; i < 10; i++ {
		p := hbase.NewPut([]byte(fmt.Sprintf("async_commit_test_%d", i))).AddValue(cf, q, []byte(fmt.Sprintf("%d", tx.GetStartTS())))
		tx.Put(themisTestTableName, p)
	}
	tx.Commit()

	tx = newTxn(s.cli).AddConfig(conf)
	for i := 0; i < 10; i++ {
		g := hbase.NewGet([]byte(fmt.Sprintf("async_commit_test_%d", i)))
		rs, err := tx.Get(themisTestTableName, g)
		c.Assert(err, IsNil)
		c.Assert(len(rs.SortedColumns), Greater, 0)
	}
}