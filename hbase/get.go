package hbase

import (
	pb "github.com/golang/protobuf/proto"
	"github.com/pingcap/go-themis/proto"

	"bytes"
	"fmt"
	"strings"
)

type Get struct {
	row        []byte
	families   [][]byte
	qualifiers [][][]byte
	versions   int32
}

func CreateNewGet(row []byte) *Get {
	return &Get{
		row: row,
		// [cf1, cf2, cf3...]
		families: make([][]byte, 0),
		// cf1 pos->[c1, c2], cf2 pos -> [c1]
		qualifiers: make([][][]byte, 0),
		versions:   1,
	}
}

func (g *Get) GetRow() []byte {
	return g.row
}

func (g *Get) AddString(famqual string) error {
	parts := strings.Split(famqual, ":")

	if len(parts) > 2 {
		return fmt.Errorf("Too many colons were found in the family:qualifier string. '%s'", famqual)
	} else if len(parts) == 2 {
		g.AddStringColumn(parts[0], parts[1])
	} else {
		g.AddStringFamily(famqual)
	}

	return nil
}

func (g *Get) AddStringColumn(family, qual string) {
	g.AddColumn([]byte(family), []byte(qual))
}

func (g *Get) AddStringFamily(family string) {
	g.AddFamily([]byte(family))
}

func (g *Get) AddColumn(family, qual []byte) {
	g.AddFamily(family)
	pos := g.posOfFamily(family)
	g.qualifiers[pos] = append(g.qualifiers[pos], qual)
}

func (g *Get) AddFamily(family []byte) {
	pos := g.posOfFamily(family)
	if pos == -1 {
		g.families = append(g.families, family)
		g.qualifiers = append(g.qualifiers, make([][]byte, 0))
	}
}

func (g *Get) posOfFamily(family []byte) int {
	for p, v := range g.families {
		if bytes.Equal(family, v) {
			return p
		}
	}
	return -1
}

func (g *Get) ToProto() pb.Message {
	get := &proto.Get{
		Row: g.row,
	}

	for i, v := range g.families {
		get.Column = append(get.Column, &proto.Column{
			Family:    v,
			Qualifier: g.qualifiers[i],
		})
	}

	get.MaxVersions = pb.Uint32(uint32(g.versions))

	return get
}
