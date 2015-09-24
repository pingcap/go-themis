package themis

import (
	"github.com/c4pt0r/go-hbase"
	"github.com/ngaut/log"
	"flag"
)

const (
	themisTestTableName string = "themis_test"
 	cfName string = "cf"
)

var zk *string = flag.String("zk", "cuiqiu-pc:2222", "hbase zookeeper info")

func createHBaseClient() (hbase.HBaseClient, error) {
	flag.Parse()
	cli, err := hbase.NewClient([]string{*zk}, "/hbase")
	if err != nil {
		return nil, err
	}

	return cli, nil
}

func createNewTableAndDropOldTable(cli hbase.HBaseClient, tblName string, family string) error {
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
	err := cli.CreateTable(t, nil)
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
