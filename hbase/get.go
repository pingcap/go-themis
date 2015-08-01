package hbase

import (
	pb "github.com/golang/protobuf/proto"
	"github.com/pingcap/go-themis/proto"

	"fmt"
	"strings"
)

type Get struct {
	Row         []byte
	Families    set
	FamilyQuals map[string]set
	Versions    int32
}

func CreateNewGet(row []byte) *Get {
	return &Get{
		Row:         row,
		Families:    newSet(),
		FamilyQuals: make(map[string]set),
		Versions:    1,
	}
}

func (g *Get) GetRow() []byte {
	return g.Row
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
	g.FamilyQuals[string(family)].add(string(qual))
}

func (g *Get) AddFamily(family []byte) {
	g.Families.add(string(family))
	if _, ok := g.FamilyQuals[string(family)]; !ok {
		g.FamilyQuals[string(family)] = newSet()
	}

}

func (g *Get) ToProto() pb.Message {
	get := &proto.Get{
		Row: g.Row,
	}

	for v, _ := range g.Families {
		col := &proto.Column{
			Family: []byte(v),
		}
		var quals [][]byte
		for qual, _ := range g.FamilyQuals[v] {
			quals = append(quals, []byte(qual))
		}
		col.Qualifier = quals
		get.Column = append(get.Column, col)
	}
	get.MaxVersions = pb.Uint32(uint32(g.Versions))
	return get
}
