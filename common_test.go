package themis

import (
	"github.com/c4pt0r/go-hbase"
	"github.com/ngaut/log"
)

const themisTestTableName string = "themis_test"
const cfName string = "cf"

func createHBaseClient() (hbase.HBaseClient, error) {
	cli, err := hbase.NewClient([]string{"cuiqiu-pc:2222"}, "/hbase")
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	return cli, nil
}

func createNewTableAndDropOldTable(cli hbase.HBaseClient, tblName string, family string) {
	if cli.TableExists(tblName) {
		dropTable(cli, tblName)
		log.Info("drop table : " + tblName)
	}

	t := hbase.NewTableDesciptor(hbase.NewTableNameWithDefaultNS(tblName))
	cf := hbase.NewColumnFamilyDescriptor(family)
	cf.AddStrAddr("THEMIS_ENABLE", "true")
	t.AddColumnDesc(cf)
	err := cli.CreateTable(t, nil)
	if err != nil {
		log.Fatal(err)
	}

}

func dropTable(cli hbase.HBaseClient, tblName string) {
	if !cli.TableExists(tblName) {
		log.Info("table not exist")
		return
	}

	t := hbase.NewTableNameWithDefaultNS(tblName)
	err := cli.DisableTable(t)
	if err != nil {
		log.Fatal(err)
	}

	err2 := cli.DropTable(t)
	if err2 != nil {
		log.Fatal(err2)
	}
}
