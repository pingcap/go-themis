package themis

import (
	"bytes"
	"encoding/binary"
)

var (
	_ ThemisLock = (*PrimaryLock)(nil)
	_ ThemisLock = (*SecondaryLock)(nil)
)

type lock struct {
	typ        Type
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
		lock: &lock{
			clientAddr: "null-client-addr",
		},
	}
}

func (l *PrimaryLock) IsExpired() bool {
	return l.lock.expired
}

func (l *PrimaryLock) GetPrimaryLock() *PrimaryLock {
	return l
}

func (l *PrimaryLock) IsPrimary() bool {
	return true
}

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

func (l *SecondaryLock) IsExpired() bool {
	return l.lock.expired
}

func (l *SecondaryLock) GetPrimaryLock() *PrimaryLock {
	return &PrimaryLock{
		lock: l.lock,
	}
}

func (l *SecondaryLock) IsPrimary() bool {
	return false
}

func (l *SecondaryLock) toBytes() []byte {
	buf := bytes.NewBuffer(nil)
	if l.IsPrimary() {
		binary.Write(buf, binary.BigEndian, uint8(1))
	} else {
		binary.Write(buf, binary.BigEndian, uint8(0))
	}

	binary.Write(buf, binary.BigEndian, byte(l.typ))
	binary.Write(buf, binary.BigEndian, int64(l.ts))
	// write client addr
	szBuf := make([]byte, 8)
	n := binary.PutUvarint(szBuf, uint64(len(l.clientAddr)))
	buf.Write(szBuf[0:n])
	buf.Write([]byte(l.clientAddr))

	binary.Write(buf, binary.BigEndian, int64(l.wallTs))
	return buf.Bytes()
}

type ThemisLock interface {
	IsExpired() bool
	IsPrimary() bool
	GetPrimaryLock() *PrimaryLock
}

func (l *lock) parseField(r *bytes.Buffer) error {
	// read type
	var typ uint8
	err := binary.Read(r, binary.BigEndian, &typ)
	if err != nil {
		return err
	}
	l.typ = Type(typ)

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
