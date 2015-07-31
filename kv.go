package themis

type Kv struct {
	row []byte
	val []byte
	ts  uint64
	column
}

func newKv(row, family, qual, val []byte) *Kv {
	return &Kv{
		row: row,
		column: column{
			family: family,
			qual:   qual,
		},
		val: val,
	}
}

func newKvWithColumn(row []byte, col column, val []byte) *Kv {
	return &Kv{
		row:    row,
		column: col,
		val:    val,
	}
}
