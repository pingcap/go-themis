package themis

import (
	"flag"
	"strings"

	"github.com/ngaut/log"
	"github.com/pingcap/go-hbase"
	"github.com/pingcap/go-themis/oracle/oracles"
)

const (
	themisTestTableName string = "themis_test"
)

var (
	testRow = []byte("test_row")
	cf      = []byte("cf")
	q       = []byte("q")
)

var (
	zk           = flag.String("zk", "localhost", "hbase zookeeper info")
	globalOracle = oracles.NewLocalOracle()
)

func newTxn(c hbase.HBaseClient, cfg TxnConfig) Txn {
	txn, err := NewTxnWithConf(c, cfg, globalOracle)
	if err != nil {
		log.Fatal(err)
	}
	return txn
}

func getTestZkHosts() []string {
	zks := strings.Split(*zk, ",")
	if len(zks) == 0 {
		log.Fatal("invalid zk")
	}
	return zks
}

func createHBaseClient() (hbase.HBaseClient, error) {
	flag.Parse()
	cli, err := hbase.NewClient(getTestZkHosts(), "/hbase")
	if err != nil {
		return nil, err
	}
	return cli, nil
}

func createNewTableAndDropOldTable(cli hbase.HBaseClient, tblName string, family string, splits [][]byte) error {
	if cli.TableExists(tblName) {
		err := dropTable(cli, tblName)
		if err != nil {
			return err
		}
		log.Info("drop table : " + tblName)
	}
	t := hbase.NewTableDesciptor(hbase.NewTableNameWithDefaultNS(tblName))
	cf := hbase.NewColumnFamilyDescriptor(family)
	cf.AddStrAddr("THEMIS_ENABLE", "true")
	t.AddColumnDesc(cf)
	err := cli.CreateTable(t, splits)
	if err != nil {
		return err
	}
	return nil
}

func dropTable(cli hbase.HBaseClient, tblName string) error {
	if !cli.TableExists(tblName) {
		log.Info("table not exist")
		return nil
	}
	t := hbase.NewTableNameWithDefaultNS(tblName)
	err := cli.DisableTable(t)
	if err != nil {
		return err
	}
	err = cli.DropTable(t)
	if err != nil {
		return err
	}
	return nil
}
