package themis

import (
	"bytes"
	"fmt"
	"io"
	"strings"
)

type column struct {
	family []byte
	qual   []byte
}

func (c *column) write(w io.Writer) {
	writeVarBytes(w, c.family)
	writeVarBytes(w, c.qual)
}

func (c *column) String() string {
	return fmt.Sprintf("%s:%s", c.family, c.qual)
}

func (c *column) parseFromString(s string) {
	pair := strings.Split(s, ":")
	c.family = []byte(pair[0])
	c.qual = []byte(pair[1])
}

type columnCoordinate struct {
	table []byte
	row   []byte
	column
}

func newColumnCoordinate(table, row, family, qual []byte) *columnCoordinate {
	return &columnCoordinate{
		table: table,
		row:   row,
		column: column{
			family: family,
			qual:   qual,
		},
	}
}

func (c *columnCoordinate) write(w io.Writer) {
	writeVarBytes(w, c.table)
	writeVarBytes(w, c.row)
	c.column.write(w)
}

func (c *columnCoordinate) equal(a *columnCoordinate) bool {
	return bytes.Compare(c.table, a.table) == 0 &&
		bytes.Compare(c.row, a.row) == 0 &&
		bytes.Compare(c.family, a.family) == 0 &&
		bytes.Compare(c.qual, a.qual) == 0
}

func (c *columnCoordinate) String() string {
	return fmt.Sprintf("%s:%s:%s:%s", c.table, c.row, c.family, c.qual)
}

func (c *columnCoordinate) parserFromString(s string) {
	parts := strings.Split(s, ":")
	if len(parts) == 4 {
		c.table = []byte(parts[0])
		c.row = []byte(parts[1])
		c.family = []byte(parts[2])
		c.qual = []byte(parts[3])
	}
}

func (c *columnCoordinate) parseField(b ByteMultiReader) error {
	table, err := readVarBytes(b)
	if err != nil {
		return err
	}
	c.table = table

	row, err := readVarBytes(b)
	if err != nil {
		return err
	}
	c.row = row

	family, err := readVarBytes(b)
	if err != nil {
		return err
	}
	c.family = family

	qual, err := readVarBytes(b)
	if err != nil {
		return err
	}
	c.qual = qual
	return nil
}

func (c *columnCoordinate) getColumn() *column {
	return &column{
		family: c.family,
		qual:   c.qual,
	}
}
