package hbase

type Type byte

const (
	TypeMinimum             = Type(0)
	TypePut                 = Type(4)
	TypeDelete              = Type(8)
	TypeDeleteFamilyVersion = Type(10)
	TypeDeleteColumn        = Type(12)
	TypeDeleteFamily        = Type(14)
	TypeMaximum             = Type(0xff)
)
