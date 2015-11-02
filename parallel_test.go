package themis

import (
	"runtime"
	"strconv"
	"sync"

	"github.com/pingcap/go-hbase"
	"github.com/ngaut/log"
	. "gopkg.in/check.v1"
)

type ParallelTestSuit struct{}

var _ = Suite(&ParallelTestSuit{})

func (s *ParallelTestSuit) TestParallelHbaseCall(c *C) {
	runtime.GOMAXPROCS(runtime.NumCPU() / 2)
	cli, err := createHBaseClient()
	c.Assert(err, Equals, nil)

	err = createNewTableAndDropOldTable(cli, themisTestTableName, cfName, nil)
	c.Assert(err, Equals, nil)

	wg := sync.WaitGroup{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tx := NewTxn(cli)
			p := hbase.NewPut([]byte("test"))
			p.AddValue([]byte(cfName), []byte("q"), []byte(strconv.Itoa(i)))
			tx.Put(themisTestTableName, p)
			tx.Commit()
		}(i)
	}
	wg.Wait()

	g := hbase.NewGet([]byte("test"))
	g.AddStringColumn(cfName, "q")
	rs, err := cli.Get(themisTestTableName, g)
	if err != nil {
		log.Fatal(err)
	}
	log.Info(string(rs.SortedColumns[0].Value))
}
