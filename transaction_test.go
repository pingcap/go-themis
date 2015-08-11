package themis

import (
	"strconv"
	"sync"

	"github.com/c4pt0r/go-hbase"
	"github.com/ngaut/log"
	. "gopkg.in/check.v1"
)

type TransactionTestSuit struct{}

var _ = Suite(&TransactionTestSuit{})

func (s *TransactionTestSuit) TestTransaction(c *C) {
	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			cli, err := hbase.NewClient([]string{"localhost"}, "/hbase")
			if err != nil {
				return
			}

			tx := NewTxn(cli)

			put := hbase.NewPut([]byte("Joe"))
			put.AddValue([]byte("Account"), []byte("cash"), []byte(strconv.Itoa(i)))

			put2 := hbase.NewPut([]byte("Bob"))
			put2.AddValue([]byte("Account"), []byte("cash"), []byte(strconv.Itoa(i)))

			put3 := hbase.NewPut([]byte("Tom"))
			put3.AddValue([]byte("Account"), []byte("cash"), []byte(strconv.Itoa(i)))

			tx.Put("CashTable", put)
			tx.Put("CashTable", put2)
			tx.Put("CashTable", put3)

			tx.Commit()
		}(i)
	}
	wg.Wait()

	cli, err := hbase.NewClient([]string{"localhost"}, "/hbase")
	c.Assert(err, Equals, nil)

	tx := NewTxn(cli)
	get := hbase.NewGet([]byte("Joe"))
	get.AddColumn([]byte("Account"), []byte("cash"))

	get2 := hbase.NewGet([]byte("Bob"))
	get2.AddColumn([]byte("Account"), []byte("cash"))

	r, err := tx.Get("CashTable", get)
	c.Assert(err, Equals, nil)
	r2, err := tx.Get("CashTable", get2)
	c.Assert(err, Equals, nil)

	rVal, _ := strconv.Atoi(string(r.SortedColumns[0].Value))
	rVal2, _ := strconv.Atoi(string(r2.SortedColumns[0].Value))
	log.Info("return val:", rVal)
	log.Info("return val2:", rVal2)
	c.Assert(rVal >= 0 && rVal < 10 && rVal == rVal2, Equals, true)

	scanner := tx.GetScanner([]byte("CashTable"), []byte("Boa"), nil)
	cnt := 0
	for {
		r := scanner.Next()
		if r == nil {
			break
		}
		cnt += 1
		log.Info(string(r.Row))
	}
	// may be all trx are failed
	c.Assert(cnt == 3 || cnt == 2, Equals, true)

	// delete
	tx = NewTxn(cli)
	d := hbase.NewDelete([]byte("Tom"))
	d.AddColumn([]byte("Account"), []byte("cash"))
	tx.Delete("CashTable", d)
	tx.Commit()

	tx = NewTxn(cli)
	scanner = tx.GetScanner([]byte("CashTable"), nil, nil)
	for {
		r := scanner.Next()
		if r == nil {
			break
		}
		log.Info(string(r.Row))
	}

}
