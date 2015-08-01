package hbase

type Kv struct {
	Row   []byte
	Ts    uint64
	Value []byte
	// history results
	Values map[uint64][]byte
	Column
}

func NewKv(row, family, qual, val []byte, ts uint64) *Kv {
	return &Kv{
		Row: row,
		Column: Column{
			Family: family,
			Qual:   qual,
		},
		Ts:    ts,
		Value: val,
	}
}

func NewKvWithColumn(row []byte, col Column, val []byte, ts uint64) *Kv {
	return &Kv{
		Row:    row,
		Column: col,
		Value:  val,
	}
}
