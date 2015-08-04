package themis

import (
	"bytes"
	"encoding/binary"
	"strings"

	"github.com/ngaut/log"
	"github.com/pingcap/go-themis/hbase"
	"github.com/pingcap/go-themis/iohelper"
	"github.com/pingcap/go-themis/proto"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"

	. "gopkg.in/check.v1"
)

type HBaseClientTestSuit struct{}

var _ = Suite(&HBaseClientTestSuit{})

const (
	DefaultSeparator byte = ':'
)

type mockHbaseClient struct {
	// row => {'cf:q' : data}
	db *leveldb.DB
}

func newMockHbase() hbaseClient {
	c := &mockHbaseClient{}
	var err error
	c.db, err = leveldb.Open(storage.NewMemStorage(), nil)
	if err != nil {
		panic(err)
	}
	return c
}

var _ hbaseClient = (*mockHbaseClient)(nil)

func composeKey(tbl string, row []byte, cf []byte, qual []byte, ts uint64) []byte {
	// tbl:row:cf:ret
	ret := make([]byte, 0, len(tbl)+len(row)+len(cf)+len(qual)+3)
	ret = append(ret, []byte(tbl)...)
	ret = append(ret, DefaultSeparator)
	ret = append(ret, row...)
	ret = append(ret, DefaultSeparator)
	ret = append(ret, cf...)
	ret = append(ret, DefaultSeparator)
	if qual != nil {
		ret = append(ret, qual...)
		ret = append(ret, DefaultSeparator)
		buf := bytes.NewBuffer(nil)
		binary.Write(buf, binary.BigEndian, ts)
		ret = append(ret, buf.Bytes()...)
	}
	return ret
}

func (c *mockHbaseClient) Iter(prefix []byte, fn func(k, v []byte) (bool, error)) error {
	var err error
	var more bool
	iter := c.db.NewIterator(util.BytesPrefix(prefix), nil)
	for iter.Next() {
		k := iter.Key()
		v := iter.Value()
		if more, err = fn(k, v); err != nil || !more {
			break
		}
	}
	iter.Release()
	if err != nil {
		return err
	}
	err = iter.Error()
	if err != nil {
		return err
	}
	return nil
}

func parseColumnInfoFormRowKey(k []byte) (cf []byte, q []byte, ts uint64) {
	parts := strings.Split(string(k), ":")
	// tbl:row:cf:q:ts
	if len(parts) != 5 {
		log.Fatal("error key format", k, string(k))
	}
	cf = []byte(parts[2])
	q = []byte(parts[3])
	tsBytes := []byte(parts[4])
	buf := bytes.NewBuffer(tsBytes)
	ts, _ = iohelper.ReadUint64(buf)
	return
}

func (c *mockHbaseClient) Get(tbl string, g *hbase.Get) (*hbase.ResultRow, error) {
	row := g.Row
	res := &hbase.ResultRow{
		Row:     row,
		Columns: map[string]*hbase.Kv{},
	}
	fn := func(k, v []byte) (bool, error) {
		log.Info(k)
		cf, q, ts := parseColumnInfoFormRowKey(k)
		col := &hbase.Kv{
			Row:   row,
			Value: v,
			Ts:    ts,
			Column: hbase.Column{
				Family: cf,
				Qual:   q,
			},
		}
		colName := col.Column.String()
		if v, exists := res.Columns[colName]; exists {
			// renew the same cf result
			if col.Ts > v.Ts {
				v.Value = col.Value
				v.Ts = col.Ts
			}
			v.Values[col.Ts] = col.Value
		} else {
			col.Values = map[uint64][]byte{col.Ts: col.Value}
			res.Columns[colName] = col
			res.SortedColumns = append(res.SortedColumns, col)
		}
		return true, nil
	}
	for family, _ := range g.Families {
		cf := family
		if len(g.FamilyQuals[family]) == 0 {
			prefix := composeKey(tbl, row, []byte(cf), nil, 0)
			c.Iter(prefix, fn)
		} else {
			for qual, _ := range g.FamilyQuals[family] {
				prefix := composeKey(tbl, row, []byte(cf), []byte(qual), 0)
				c.Iter(prefix, fn)
			}
		}
	}
	return res, nil
}

func (c *mockHbaseClient) Put(tbl string, p *hbase.Put) (bool, error) {
	row := p.Row
	for i, family := range p.Families {
		cf := family
		for j, _ := range p.Qualifiers[i] {
			qual := p.Qualifiers[i][j]
			val := p.Values[i][j]
			key := composeKey(tbl, row, cf, qual, 0)
			log.Info("put", string(key), string(val))
			err := c.db.Put(key, val, nil)
			if err != nil {
				return false, err
			}
		}
	}
	return true, nil
}

func (c *mockHbaseClient) Delete(tbl string, d *hbase.Delete) (bool, error) {
	row := d.Row
	for family, _ := range d.Families {
		cf := family
		// delete whole family
		if len(d.FamilyQuals[family]) == 0 {
			prefix := composeKey(tbl, row, []byte(cf), nil, 0)
			// delete all this family

			batch := new(leveldb.Batch)
			iter := c.db.NewIterator(util.BytesPrefix(prefix), nil)
			for iter.Next() {
				batch.Delete(iter.Key())
			}
			iter.Release()
			err := iter.Error()
			if err != nil {
				return false, err
			}
			err = c.db.Write(batch, nil)
			if err != nil {
				return false, err
			}
		}
		// delete cf:qual
		for qual, _ := range d.FamilyQuals[family] {
			k := composeKey(tbl, row, []byte(cf), []byte(qual), 0)
			c.db.Delete(k, nil)
		}
	}
	return true, nil
}

func (c *mockHbaseClient) ServiceCall(table string, call *hbase.CoprocessorServiceCall) (*proto.CoprocessorServiceResponse, error) {
	return nil, nil
}

func (s *HBaseClientTestSuit) TestMockHbaseClient(c *C) {
	cli := newMockHbase()
	p := hbase.CreateNewPut([]byte("row"))
	p.AddValue([]byte("cf"), []byte("qual"), []byte("val"))
	ok, err := cli.Put("tbl", p)
	c.Assert(err, Equals, nil)
	c.Assert(ok, Equals, true)

	p = hbase.CreateNewPut([]byte("row1"))
	p.AddValue([]byte("cf"), []byte("qual"), []byte("val"))
	p.AddValue([]byte("cf"), []byte("qual2"), []byte("val"))
	cli.Put("tbl", p)

	d := hbase.CreateNewDelete([]byte("row"))
	d.AddFamily([]byte("cf"))
	cli.Delete("tbl", d)

	g := hbase.CreateNewGet([]byte("row1"))
	g.AddFamily([]byte("cf"))
	r, err := cli.Get("tbl", g)
	c.Assert(err, Equals, nil)
	c.Assert(len(r.SortedColumns), Equals, 2)
	c.Assert(string(r.SortedColumns[0].Value), Equals, "val")
	log.Info(r)
}
