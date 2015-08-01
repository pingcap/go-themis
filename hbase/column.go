package hbase

import (
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/pingcap/go-themis/iohelper"
)

type Column struct {
	Family []byte
	Qual   []byte
}

func NewColumn(family, qual []byte) *Column {
	return &Column{
		Family: family,
		Qual:   qual,
	}
}

func (c *Column) Write(w io.Writer) {
	iohelper.WriteVarBytes(w, c.Family)
	iohelper.WriteVarBytes(w, c.Qual)
}

func (c *Column) String() string {
	return fmt.Sprintf("%s:%s", c.Family, c.Qual)
}

func (c *Column) ParseFromString(s string) {
	pair := strings.Split(s, ":")
	c.Family = []byte(pair[0])
	c.Qual = []byte(pair[1])
}

type ColumnCoordinate struct {
	Table []byte
	Row   []byte
	Column
}

func NewColumnCoordinate(table, row, family, qual []byte) *ColumnCoordinate {
	return &ColumnCoordinate{
		Table: table,
		Row:   row,
		Column: Column{
			Family: family,
			Qual:   qual,
		},
	}
}

func (c *ColumnCoordinate) Write(w io.Writer) {
	iohelper.WriteVarBytes(w, c.Table)
	iohelper.WriteVarBytes(w, c.Row)
	c.Column.Write(w)
}

func (c *ColumnCoordinate) Equal(a *ColumnCoordinate) bool {
	return bytes.Compare(c.Table, a.Table) == 0 &&
		bytes.Compare(c.Row, a.Row) == 0 &&
		bytes.Compare(c.Family, a.Family) == 0 &&
		bytes.Compare(c.Qual, a.Qual) == 0
}

func (c *ColumnCoordinate) String() string {
	return fmt.Sprintf("%s:%s:%s:%s", c.Table, c.Row, c.Family, c.Qual)
}

func (c *ColumnCoordinate) ParseFromString(s string) {
	parts := strings.Split(s, ":")
	if len(parts) == 4 {
		c.Table = []byte(parts[0])
		c.Row = []byte(parts[1])
		c.Family = []byte(parts[2])
		c.Qual = []byte(parts[3])
	}
}

func (c *ColumnCoordinate) ParseField(b iohelper.ByteMultiReader) error {
	table, err := iohelper.ReadVarBytes(b)
	if err != nil {
		return err
	}
	c.Table = table

	row, err := iohelper.ReadVarBytes(b)
	if err != nil {
		return err
	}
	c.Row = row

	family, err := iohelper.ReadVarBytes(b)
	if err != nil {
		return err
	}
	c.Family = family

	qual, err := iohelper.ReadVarBytes(b)
	if err != nil {
		return err
	}
	c.Qual = qual
	return nil
}

func (c *ColumnCoordinate) GetColumn() *Column {
	return &Column{
		Family: c.Family,
		Qual:   c.Qual,
	}
}
