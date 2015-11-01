package themis

import (
	"flag"
	"strings"

	"github.com/c4pt0r/go-hbase"
	"github.com/ngaut/log"
)

const (
	themisTestTableName string = "themis_test"
	cfName              string = "cf"
)

var (
	zk = flag.String("zk", "localhost", "hbase zookeeper info")
)

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
