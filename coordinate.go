package themis

import "fmt"

type column struct {
	family []byte
	qual   []byte
}

func (c *column) String() string {
	return fmt.Sprintf("%s:%s", c.family, c.qual)
}

func columnFromString(s string) *column {
	var f, q string
	fmt.Sscanf(s, "%s:%s", &f, &q)
	return &column{
		family: []byte(f),
		qual:   []byte(q),
	}
}

type columnCoordinate struct {
	table []byte
	row   []byte
	column
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
