package themis

import (
	"bytes"
	"encoding/binary"
)

type SecondaryLock struct {
	*lock
	primaryCoordinate *columnCoordinate
}

func newSecondaryLock() *SecondaryLock {
	return &SecondaryLock{
		lock: &lock{
			clientAddr: "null-client-addr",
		},
		primaryCoordinate: &columnCoordinate{},
	}
}

func (l *SecondaryLock) isExpired() bool {
	return l.lock.expired
}

func (l *SecondaryLock) getPrimaryColumn() *columnCoordinate {
	return l.primaryCoordinate
}

func (l *SecondaryLock) isPrimary() bool {
	return false
}

func (l *SecondaryLock) toBytes() []byte {
	buf := bytes.NewBuffer(nil)
	binary.Write(buf, binary.BigEndian, uint8(0))
	l.lock.write(buf)
	l.primaryCoordinate.write(buf)
	return buf.Bytes()
}

func (l *SecondaryLock) parseField(r ByteMultiReader) error {
	l.lock.parseField(r)
	primary := &columnCoordinate{}
	err := primary.parseField(r)
	if err != nil {
		return err
	}
	l.primaryCoordinate = primary
	return nil
}
