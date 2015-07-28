package themis

import "bytes"

type columnCoordinate struct {
	table  []byte
	row    []byte
	family []byte
	qual   []byte
}

func (c *columnCoordinate) parseField(b *bytes.Buffer) error {
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
