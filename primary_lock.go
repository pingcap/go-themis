package themis

import (
	"bytes"
	"encoding/binary"
)

type PrimaryLock struct {
	*lock
	// {coordinate => type}
	secondaries map[string]Type
}

func newPrimaryLock() *PrimaryLock {
	return &PrimaryLock{
		lock: &lock{
			clientAddr: "null-client-addr",
		},
		secondaries: map[string]Type{},
	}
}

func (l *PrimaryLock) addSecondaryColumn(col *columnCoordinate, t Type) {
	l.secondaries[col.String()] = t
}

func (l *PrimaryLock) toBytes() []byte {
	buf := bytes.NewBuffer(nil)
	// set is primary
	binary.Write(buf, binary.BigEndian, uint8(1))
	l.lock.write(buf)

	// write secondaries
	binary.Write(buf, binary.BigEndian, int32(len(l.secondaries)))
	for k, v := range l.secondaries {
		c := &columnCoordinate{}
		c.parserFromString(k)
		c.write(buf)
		buf.WriteByte(uint8(v))
	}
	return buf.Bytes()
}

func (l *PrimaryLock) isExpired() bool {
	return l.lock.expired
}

func (l *PrimaryLock) getSecondaryColumnType(c *columnCoordinate) Type {
	v, ok := l.secondaries[c.String()]
	if !ok {
		return TypeMinimum
	}
	return v
}

func (l *PrimaryLock) isPrimary() bool {
	return true
}

func (l *PrimaryLock) parseField(buf ByteMultiReader) error {
	l.lock.parseField(buf)
	var sz int32
	err := binary.Read(buf, binary.BigEndian, &sz)
	if err != nil {
		return err
	}
	for i := 0; i < int(sz); i++ {
		c := &columnCoordinate{}
		c.parseField(buf)
		b, err := buf.ReadByte()
		if err != nil {
			return err
		}
		t := Type(b)
		l.addSecondaryColumn(c, t)
	}
	return nil
}
