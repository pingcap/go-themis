package hbase

import (
	pb "github.com/golang/protobuf/proto"
	"github.com/pingcap/go-themis/proto"

	"bytes"
)

type Put struct {
	row        []byte
	families   [][]byte
	qualifiers [][][]byte
	values     [][][]byte
}

func CreateNewPut(row []byte) *Put {
	return &Put{
		row:        row,
		families:   make([][]byte, 0),
		qualifiers: make([][][]byte, 0),
		values:     make([][][]byte, 0),
	}
}

func (p *Put) GetRow() []byte {
	return p.row
}

func (p *Put) AddValue(family, qual, value []byte) {
	pos := p.posOfFamily(family)

	if pos == -1 {
		p.families = append(p.families, family)
		p.qualifiers = append(p.qualifiers, make([][]byte, 0))
		p.values = append(p.values, make([][]byte, 0))

		pos = p.posOfFamily(family)
	}

	p.qualifiers[pos] = append(p.qualifiers[pos], qual)
	p.values[pos] = append(p.values[pos], value)
}

func (p *Put) AddStringValue(family, column, value string) {
	p.AddValue([]byte(family), []byte(column), []byte(value))
}

func (p *Put) posOfFamily(family []byte) int {
	for p, v := range p.families {
		if bytes.Equal(family, v) {
			return p
		}
	}
	return -1
}

/*
func (p *Put) Entries() []*columnMutation {
	var ret []*columnMutation
	for i, f := range p.families {
		qualifiers := p.qualifiers[i]
		for j, q := range qualifiers {
			mutation := &columnMutation{
				column: &column{
					family: f,
					qual:   q,
				},
				mutationValuePair: &mutationValuePair{
					typ:   TypePut,
					value: p.values[i][j],
				},
			}
			ret = append(ret, mutation)
		}
	}
	return ret
}
*/

func (p *Put) ToProto() pb.Message {
	put := &proto.MutationProto{
		Row:        p.row,
		MutateType: proto.MutationProto_PUT.Enum(),
	}

	for i, family := range p.families {
		cv := &proto.MutationProto_ColumnValue{
			Family: family,
		}

		for j, _ := range p.qualifiers[i] {
			cv.QualifierValue = append(cv.QualifierValue, &proto.MutationProto_ColumnValue_QualifierValue{
				Qualifier: p.qualifiers[i][j],
				Value:     p.values[i][j],
			})
		}

		put.ColumnValue = append(put.ColumnValue, cv)
	}

	return put
}
