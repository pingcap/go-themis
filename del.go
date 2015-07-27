package themis

import (
	pb "github.com/golang/protobuf/proto"
	"github.com/pingcap/go-themis/proto"

	"bytes"
	"fmt"
	"math"
	"strings"
)

type Delete struct {
	key        []byte
	families   [][]byte
	qualifiers [][][]byte
}

func CreateNewDelete(key []byte) *Delete {
	return &Delete{
		key:        key,
		families:   make([][]byte, 0),
		qualifiers: make([][][]byte, 0),
	}
}

func (d *Delete) AddString(famqual string) error {
	parts := strings.Split(famqual, ":")

	if len(parts) > 2 {
		return fmt.Errorf("Too many colons were found in the family:qualifier string. '%s'", famqual)
	} else if len(parts) == 2 {
		d.AddStringColumn(parts[0], parts[1])
	} else {
		d.AddStringFamily(famqual)
	}

	return nil
}

func (d *Delete) AddStringColumn(family, qual string) {
	d.AddColumn([]byte(family), []byte(qual))
}

func (d *Delete) AddStringFamily(family string) {
	d.AddFamily([]byte(family))
}

func (d *Delete) AddColumn(family, qual []byte) {
	d.AddFamily(family)
	pos := d.posOfFamily(family)
	d.qualifiers[pos] = append(d.qualifiers[pos], qual)
}

func (d *Delete) AddFamily(family []byte) {
	pos := d.posOfFamily(family)

	if pos == -1 {
		d.families = append(d.families, family)
		d.qualifiers = append(d.qualifiers, make([][]byte, 0))
	}
}

func (d *Delete) posOfFamily(family []byte) int {
	for p, v := range d.families {
		if bytes.Equal(family, v) {
			return p
		}
	}
	return -1
}

func (d *Delete) toProto() pb.Message {
	del := &proto.MutationProto{
		Row:        d.key,
		MutateType: proto.MutationProto_DELETE.Enum(),
	}

	for i, v := range d.families {
		cv := &proto.MutationProto_ColumnValue{
			Family:         v,
			QualifierValue: make([]*proto.MutationProto_ColumnValue_QualifierValue, 0),
		}

		if len(d.qualifiers[i]) == 0 {
			cv.QualifierValue = append(cv.QualifierValue, &proto.MutationProto_ColumnValue_QualifierValue{
				Qualifier:  nil,
				Timestamp:  pb.Uint64(uint64(math.MaxInt64)),
				DeleteType: proto.MutationProto_DELETE_FAMILY.Enum(),
			})
		}

		for _, v := range d.qualifiers[i] {
			cv.QualifierValue = append(cv.QualifierValue, &proto.MutationProto_ColumnValue_QualifierValue{
				Qualifier:  v,
				Timestamp:  pb.Uint64(uint64(math.MaxInt64)),
				DeleteType: proto.MutationProto_DELETE_MULTIPLE_VERSIONS.Enum(),
			})
		}

		del.ColumnValue = append(del.ColumnValue, cv)
	}

	return del
}

func (c *Client) Delete(table string, del *Delete) (bool, error) {
	ch := c.action([]byte(table), del.key, del, true, 0)

	response := <-ch
	switch r := response.(type) {
	case *proto.MutateResponse:
		return r.GetProcessed(), nil
	}

	return false, fmt.Errorf("No valid response seen [response: %#v]", response)
}
