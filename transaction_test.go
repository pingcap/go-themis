package themis

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/c4pt0r/go-hbase"
	"github.com/ngaut/log"
	. "gopkg.in/check.v1"
)

type TransactionTestSuit struct {
	cli hbase.HBaseClient
}

var _ = Suite(&TransactionTestSuit{})

func (s *TransactionTestSuit) SetUpSuite(c *C) {
	s.cli, _ = createHBaseClient()
	err := createNewTableAndDropOldTable(s.cli, themisTestTableName, cfName)
	c.Assert(err, Equals, nil)
}

func (s *TransactionTestSuit) TestTransaction(c *C) {
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tx := NewTxn(s.cli)

			put := hbase.NewPut([]byte("Joe"))
			put.AddValue([]byte(cfName), []byte("q"), []byte(strconv.Itoa(i)))

			put2 := hbase.NewPut([]byte("Bob"))
			put2.AddValue([]byte(cfName), []byte("q"), []byte(strconv.Itoa(i)))

			put3 := hbase.NewPut([]byte("Tom"))
			put3.AddValue([]byte(cfName), []byte("q"), []byte(strconv.Itoa(i)))

			tx.Put(themisTestTableName, put)
			tx.Put(themisTestTableName, put2)
			tx.Put(themisTestTableName, put3)

			tx.Commit()
		}(i)
	}
	wg.Wait()

	tx := NewTxn(s.cli)
	get := hbase.NewGet([]byte("Joe"))
	get.AddColumn([]byte(cfName), []byte("q"))

	get2 := hbase.NewGet([]byte("Bob"))
	get2.AddColumn([]byte(cfName), []byte("q"))

	r, err := tx.Get(themisTestTableName, get)
	c.Assert(err, Equals, nil)
	r2, err := tx.Get(themisTestTableName, get2)
	c.Assert(err, Equals, nil)

	if r != nil && r2 != nil {
		rVal, _ := strconv.Atoi(string(r.SortedColumns[0].Value))
		rVal2, _ := strconv.Atoi(string(r2.SortedColumns[0].Value))
		log.Info("return val:", rVal)
		log.Info("return val2:", rVal2)
		c.Assert(rVal >= 0 && rVal < 10 && rVal == rVal2, Equals, true)
	} else {
		// maybe both of the transctions are failed
		c.Assert(r == nil, Equals, true)
		c.Assert(r2 == nil, Equals, true)
	}

	scanner := tx.GetScanner([]byte(themisTestTableName), []byte("Boa"), []byte("Tom\xff"))
	cnt := 0
	for {
		r := scanner.Next()
		if r == nil {
			break
		}
		cnt += 1
		log.Info(string(r.Row))
	}
	log.Info(cnt)
	// may be all trx are failed
	c.Assert(cnt == 0 || cnt == 3, Equals, true)

	// delete
	tx = NewTxn(s.cli)
	d := hbase.NewDelete([]byte("Tom"))
	d.AddColumn([]byte(cfName), []byte("q"))
	tx.Delete(themisTestTableName, d)

	d = hbase.NewDelete([]byte("Joe"))
	d.AddColumn([]byte(cfName), []byte("q"))
	tx.Delete(themisTestTableName, d)

	d = hbase.NewDelete([]byte("Bob"))
	d.AddColumn([]byte(cfName), []byte("q"))
	tx.Delete(themisTestTableName, d)
	tx.Commit()

	tx = NewTxn(s.cli)
	scanner = tx.GetScanner([]byte(themisTestTableName), nil, nil)
	for {
		r := scanner.Next()
		if r == nil {
			break
		}
		log.Info(string(r.Row))
	}

}

func (s *TransactionTestSuit) TestAsyncCommit(c *C) {
	p := hbase.NewPut([]byte("test"))
	p.AddValue([]byte(cfName), []byte("q"), []byte("val"))
	tx := NewTxn(s.cli)
	tx.Put(themisTestTableName, p)
	tx.Commit()

	tx = NewTxn(s.cli)
	d := hbase.NewDelete([]byte("test"))
	d.AddColumn([]byte(cfName), []byte("q"))
	tx.Delete(themisTestTableName, d)
	tx.Commit()

	tx = NewTxn(s.cli)
	g := hbase.NewGet([]byte("test"))
	g.AddFamily([]byte(cfName))
	r, err := tx.Get(themisTestTableName, g)
	c.Assert(err, Equals, nil)
	c.Assert(r == nil, Equals, true)
	tx.Commit()

	conf := TxnConfig{
		ConcurrentPrewriteAndCommit: true,
		brokenCommitTest:            true,
	}

	tx = NewTxn(s.cli).AddConfig(conf)
	// simulating broken commit
	for i := 0; i < 10; i++ {
		p := hbase.NewPut([]byte(fmt.Sprintf("test_%d", i)))
		p.AddValue([]byte(cfName), []byte("q"), []byte(fmt.Sprintf("%d", tx.startTs)))
		tx.Put(themisTestTableName, p)
	}
	err = tx.Commit()
	c.Assert(err, Equals, nil)

	//  wait until lock expired.
	log.Warn("Wait for lock expired. Sleep 30s...")
	tick := 30
	for tick > 0 {
		time.Sleep(1 * time.Second)
		tick--
		log.Infof("remain %ds...", tick)
	}

	log.Warn("Try commit again")
	// new transction will not see lock
	tx = NewTxn(s.cli)
	for i := 0; i < 5; i++ {
		p := hbase.NewPut([]byte(fmt.Sprintf("test_%d", i)))
		p.AddValue([]byte(cfName), []byte("q"), []byte(fmt.Sprintf("%d", tx.startTs)))
		tx.Put(themisTestTableName, p)
	}
	err = tx.Commit()
	if err != nil {
		log.Error(err)
	}
	c.Assert(err, Equals, nil)

	tx = NewTxn(s.cli)
	for i := 5; i < 10; i++ {
		p := hbase.NewPut([]byte(fmt.Sprintf("test_%d", i)))
		p.AddValue([]byte(cfName), []byte("q"), []byte(fmt.Sprintf("%d", tx.startTs)))
		tx.Put(themisTestTableName, p)
	}
	err = tx.Commit()
	if err != nil {
		log.Error(err)
	}
	c.Assert(err, Equals, nil)

}

func (s *TransactionTestSuit) TestBrokenPrewriteSecondary(c *C) {
	// TODO: check rallback & cleanup locks
	conf := TxnConfig{
		brokenPrewriteSecondaryTest: true,
	}
	tx := NewTxn(s.cli).AddConfig(conf)
	ts := tx.startTs
	// simulating broken commit
	for i := 0; i < 10; i++ {
		p := hbase.NewPut([]byte(fmt.Sprintf("test_%d", i)))
		p.AddValue([]byte(cfName), []byte("q"), []byte(fmt.Sprintf("%d", ts)))
		tx.Put(themisTestTableName, p)
	}
	err := tx.Commit()
	c.Assert(err, NotNil)
	log.Error(err)

	// check if locks are cleaned successfully
	tx = NewTxn(s.cli)
	for i := 0; i < 10; i++ {
		g := hbase.NewGet([]byte(fmt.Sprintf("test_%d", i)))
		r, err := tx.Get(themisTestTableName, g)
		c.Assert(err, Equals, nil)
		c.Assert(string(r.SortedColumns[0].Value) != fmt.Sprintf("%d", ts), Equals, true)
	}
}

func (s *TransactionTestSuit) TestPrimaryLockTimeout(c *C) {
	// TODO: check if lock can be cleaned up when secondary prewrite failed and
	// rollback is also failed
	conf := TxnConfig{
		brokenPrewriteSecondaryTest:            true,
		brokenPrewriteSecondaryAndRollbackTest: true,
	}
	tx := NewTxn(s.cli).AddConfig(conf)
	ts := tx.startTs
	// simulating broken commit
	for i := 0; i < 10; i++ {
		p := hbase.NewPut([]byte(fmt.Sprintf("test_%d", i)))
		p.AddValue([]byte(cfName), []byte("q"), []byte(fmt.Sprintf("%d", ts)))
		tx.Put(themisTestTableName, p)
	}
	err := tx.Commit()
	c.Assert(err, NotNil)
	log.Error(err)

	//  wait until lock expired.
	log.Warn("Wait for lock expired. Sleep 30s...")
	tick := 30
	for tick > 0 {
		time.Sleep(1 * time.Second)
		tick--
		log.Infof("remain %ds...", tick)
	}

	// check if locks are cleaned successfully
	tx = NewTxn(s.cli)
	for i := 0; i < 10; i++ {
		g := hbase.NewGet([]byte(fmt.Sprintf("test_%d", i)))
		r, err := tx.Get(themisTestTableName, g)
		c.Assert(err, Equals, nil)
		c.Assert(string(r.SortedColumns[0].Value) != fmt.Sprintf("%d", ts), Equals, true)
		log.Info(r, err)
	}

}

func (s *TransactionTestSuit) TestLockRow(c *C) {
	tx := NewTxn(s.cli)

	row := []byte("Joe")
	put := hbase.NewPut(row)
	put.AddValue([]byte(cfName), []byte("q"), []byte("v"))
	tx.Put(themisTestTableName, put)
	tx.Commit()

	checkCommitSuccess(s, c, row)

	tx = NewTxn(s.cli)
	err := tx.LockRow(themisTestTableName, row)
	c.Assert(err, Equals, nil)

	tx.selectPrepareAndSecondary()
	err = tx.prewritePrimary()
	c.Assert(err, Equals, nil)
	colMap := make(map[string]string)
	colMap["#p:"+cfName+"#q"] = ""
	colMap[cfName+":q"] = ""
	colMap["L:"+cfName+"#q"] = ""
	var r *hbase.ResultRow
	r, err = tx.client.Get(themisTestTableName, hbase.NewGet(row))
	c.Assert(err, Equals, nil)
	c.Assert(3, Equals, len(r.Columns))
	for _, v := range r.Columns {
		_, exist := colMap[string(v.Family)+":"+string(v.Qual)]
		c.Assert(exist, Equals, true)
	}
	tx.commitTs = tx.startTs + 1
	tx.commitPrimary()
	checkCommitSuccess(s, c, row)

}

func checkCommitSuccess(s *TransactionTestSuit, c *C, row []byte) {
	tx := NewTxn(s.cli)
	colMap := make(map[string]string)
	colMap["#p:"+cfName+"#q"] = ""
	colMap[cfName+":q"] = ""
	r, err := tx.client.Get(themisTestTableName, hbase.NewGet(row))
	c.Assert(err, Equals, nil)
	c.Assert(2, Equals, len(r.Columns))
	for _, v := range r.Columns {
		_, exist := colMap[string(v.Family)+":"+string(v.Qual)]
		c.Assert(exist, Equals, true)
	}
}
