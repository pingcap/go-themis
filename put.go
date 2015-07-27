package themis

import (
	"fmt"

	pb "github.com/golang/protobuf/proto"
	"github.com/pingcap/go-themis/proto"

	"bytes"
)

type Put struct {
	key        []byte
	families   [][]byte
	qualifiers [][][]byte
	values     [][][]byte
}

func CreateNewPut(key []byte) *Put {
	return &Put{
		key:        key,
		families:   make([][]byte, 0),
		qualifiers: make([][][]byte, 0),
		values:     make([][][]byte, 0),
	}
}

func (p *Put) AddValue(family, column, value []byte) {
	pos := p.posOfFamily(family)

	if pos == -1 {
		p.families = append(p.families, family)
		p.qualifiers = append(p.qualifiers, make([][]byte, 0))
		p.values = append(p.values, make([][]byte, 0))

		pos = p.posOfFamily(family)
	}

	p.qualifiers[pos] = append(p.qualifiers[pos], column)
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

func (p *Put) toProto() pb.Message {
	put := &proto.MutationProto{
		Row:        p.key,
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

func (c *Client) Put(table string, put *Put) (bool, error) {
	ch := c.action([]byte(table), put.key, put, true, 0)

	response := <-ch
	switch r := response.(type) {
	case *proto.MutateResponse:
		return r.GetProcessed(), nil
	}

	return false, fmt.Errorf("No valid response seen [response: %#v]", response)
}
