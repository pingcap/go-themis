package themis

import (
	"fmt"
	"sort"

	"github.com/ngaut/log"
	"github.com/pingcap/go-themis/hbase"
	"github.com/pingcap/go-themis/proto"
)

type mutationValuePair struct {
	typ   hbase.Type
	value []byte
}

func (mp *mutationValuePair) String() string {
	return fmt.Sprintf("type: %d value: %s", mp.typ, mp.value)
}

type columnMutation struct {
	*hbase.Column
	*mutationValuePair
}

func getEntriesFromPut(p *hbase.Put) []*columnMutation {
	var ret []*columnMutation
	for i, f := range p.Families {
		qualifiers := p.Qualifiers[i]
		for j, q := range qualifiers {
			mutation := &columnMutation{
				Column: &hbase.Column{
					Family: f,
					Qual:   q,
				},
				mutationValuePair: &mutationValuePair{
					typ:   hbase.TypePut,
					value: p.Values[i][j],
				},
			}
			ret = append(ret, mutation)
		}
	}
	return ret
}

func (cm *columnMutation) toCell() *proto.Cell {
	ret := &proto.Cell{
		Family:    cm.Family,
		Qualifier: cm.Qual,
		Value:     cm.value,
	}
	if cm.typ == hbase.TypePut {
		ret.CellType = proto.CellType_PUT.Enum()
	} else {
		ret.CellType = proto.CellType_DELETE.Enum()
	}
	return ret
}

type rowMutation struct {
	tbl []byte
	row []byte
	// mutations := { 'cf:col' => mutationValuePair }
	mutations map[string]*mutationValuePair
}

func (r *rowMutation) getSize() int {
	return len(r.mutations)
}

func (r *rowMutation) getType(c hbase.Column) hbase.Type {
	p, ok := r.mutations[c.String()]
	if !ok {
		return hbase.TypeMinimum
	}
	return p.typ
}

func newRowMutation(tbl, row []byte) *rowMutation {
	return &rowMutation{
		tbl:       tbl,
		row:       row,
		mutations: map[string]*mutationValuePair{},
	}
}

func (r *rowMutation) addMutation(c *hbase.Column, typ hbase.Type, val []byte) {
	r.mutations[c.String()] = &mutationValuePair{
		typ:   typ,
		value: val,
	}
}

func (r *rowMutation) mutationList() []*columnMutation {
	var ret []*columnMutation
	var keys []string
	for k, _ := range r.mutations {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := r.mutations[k]
		c := &hbase.Column{}
		c.ParseFromString(k)
		ret = append(ret, &columnMutation{
			Column:            c,
			mutationValuePair: v,
		})
	}
	log.Warning(string(r.row))
	return ret
}

type columnMutationCache struct {
	// mutations => {table => { rowKey => row mutations } }
	mutations map[string]map[string]*rowMutation
}

func newColumnMutationCache() *columnMutationCache {
	return &columnMutationCache{
		mutations: map[string]map[string]*rowMutation{},
	}
}

func (c *columnMutationCache) addMutation(tbl []byte, row []byte, col *hbase.Column, t hbase.Type, v []byte) {
	tblRowMutations, ok := c.mutations[string(tbl)]
	if !ok {
		// create table mutation map
		tblRowMutations = map[string]*rowMutation{}
		c.mutations[string(tbl)] = tblRowMutations
	}

	rowMutations, ok := tblRowMutations[string(row)]
	if !ok {
		// create row mutation map
		rowMutations = newRowMutation(tbl, row)
		tblRowMutations[string(row)] = rowMutations
	}
	rowMutations.addMutation(col, t, v)
}

func (c *columnMutationCache) getMutation(cc *hbase.ColumnCoordinate) *mutationValuePair {
	t, ok := c.mutations[string(cc.Table)]
	if !ok {
		return nil
	}
	rowMutation, ok := t[string(cc.Row)]
	if !ok {
		return nil
	}
	p, ok := rowMutation.mutations[cc.GetColumn().String()]
	if !ok {
		return nil
	}
	return p
}

func (c *columnMutationCache) getSize() int {
	ret := 0
	for _, v := range c.mutations {
		for _, vv := range v {
			ret += len(vv.mutationList())
		}
	}
	return ret
}
