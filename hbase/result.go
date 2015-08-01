package hbase

import "github.com/pingcap/go-themis/proto"

type ResultRow struct {
	Row           []byte
	Columns       map[string]*Kv
	SortedColumns []*Kv
}

func NewResultRow(result *proto.Result) *ResultRow {
	res := &ResultRow{}
	res.Columns = make(map[string]*Kv)
	res.SortedColumns = make([]*Kv, 0)

	for _, cell := range result.GetCell() {
		res.Row = cell.GetRow()

		col := &Kv{
			Column: Column{
				Family: cell.GetFamily(),
				Qual:   cell.GetQualifier(),
			},
			Value: cell.GetValue(),
			Ts:    cell.GetTimestamp(),
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
	}
	return res
}
