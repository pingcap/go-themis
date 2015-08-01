package hbase

import (
	pb "github.com/golang/protobuf/proto"
	"github.com/pingcap/go-themis/proto"

	"bytes"
	"fmt"
	"math"
	"strings"
)

type Delete struct {
	Row        []byte
	Families   [][]byte
	Qualifiers [][][]byte
}

func CreateNewDelete(row []byte) *Delete {
	return &Delete{
		Row:        row,
		Families:   make([][]byte, 0),
		Qualifiers: make([][][]byte, 0),
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

func (d *Delete) GetRow() []byte {
	return d.Row
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
	d.Qualifiers[pos] = append(d.Qualifiers[pos], qual)
}

func (d *Delete) AddFamily(family []byte) {
	pos := d.posOfFamily(family)

	if pos == -1 {
		d.Families = append(d.Families, family)
		d.Qualifiers = append(d.Qualifiers, make([][]byte, 0))
	}
}

func (d *Delete) posOfFamily(family []byte) int {
	for p, v := range d.Families {
		if bytes.Equal(family, v) {
			return p
		}
	}
	return -1
}

func (d *Delete) ToProto() pb.Message {
	del := &proto.MutationProto{
		Row:        d.Row,
		MutateType: proto.MutationProto_DELETE.Enum(),
	}

	for i, v := range d.Families {
		cv := &proto.MutationProto_ColumnValue{
			Family:         v,
			QualifierValue: make([]*proto.MutationProto_ColumnValue_QualifierValue, 0),
		}

		if len(d.Qualifiers[i]) == 0 {
			cv.QualifierValue = append(cv.QualifierValue, &proto.MutationProto_ColumnValue_QualifierValue{
				Qualifier:  nil,
				Timestamp:  pb.Uint64(uint64(math.MaxInt64)),
				DeleteType: proto.MutationProto_DELETE_FAMILY.Enum(),
			})
		}

		for _, v := range d.Qualifiers[i] {
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
