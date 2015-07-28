package themis

import (
	"bytes"
	"encoding/binary"
)

type LockType byte

const (
	TypeMinimum             = LockType(0)
	TypePut                 = LockType(4)
	TypeDelete              = LockType(8)
	TypeDeleteFamilyVersion = LockType(10)
	TypeDeleteColumn        = LockType(12)
	TypeDeleteFamily        = LockType(14)
	TypeMaximum             = LockType(0xff)
)

type lock struct {
	typ        LockType
	ts         uint64
	wallTs     uint64
	clientAddr string
	expired    bool
}

type PrimaryLock struct {
	*lock
}

func newPrimaryLock() *PrimaryLock {
	return &PrimaryLock{
		lock: &lock{},
	}
}

type SecondaryLock struct {
	*lock
	primaryCoordinate *columnCoordinate
}

func newSecondaryLock() *SecondaryLock {
	return &SecondaryLock{
		lock:              &lock{},
		primaryCoordinate: &columnCoordinate{},
	}
}

type ThemisLock interface{}

func (l *lock) parseField(r *bytes.Buffer) error {
	// read type
	var typ uint8
	err := binary.Read(r, binary.BigEndian, &typ)
	if err != nil {
		return err
	}
	l.typ = LockType(typ)

	// read ts
	var ts int64
	err = binary.Read(r, binary.BigEndian, &ts)
	if err != nil {
		return err
	}
	l.ts = uint64(ts)

	// read client addr
	sz, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	addr := make([]byte, sz)
	r.Read(addr)
	l.clientAddr = string(addr)

	// read wall time
	var wallTs int64
	err = binary.Read(r, binary.BigEndian, &wallTs)
	if err != nil {
		return err
	}
	l.wallTs = uint64(wallTs)
	return nil
}

func parseLockFromBytes(b []byte) (ThemisLock, error) {
	buf := bytes.NewBuffer(b)
	var isPrimary uint8
	err := binary.Read(buf, binary.BigEndian, &isPrimary)
	if err != nil {
		return nil, err
	}
	if isPrimary == 1 {
		ret := newPrimaryLock()
		err = ret.parseField(buf)
		if err != nil {
			return nil, err
		}
		return ret, nil
	} else {
		ret := newSecondaryLock()
		err = ret.parseField(buf)
		if err != nil {
			return nil, err
		}
		err = ret.primaryCoordinate.parseField(buf)
		if err != nil {
			return nil, err
		}
		return ret, nil
	}
	return nil, nil
}
