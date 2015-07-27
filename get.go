package themis

import (
	pb "github.com/golang/protobuf/proto"
	"github.com/ngaut/log"
	"github.com/pingcap/go-themis/proto"

	"bytes"
	"fmt"
	"strings"
)

type Get struct {
	key        []byte
	families   [][]byte
	qualifiers [][][]byte
	versions   int32
}

func CreateNewGet(key []byte) *Get {
	return &Get{
		key:        key,
		families:   make([][]byte, 0),
		qualifiers: make([][][]byte, 0),
		versions:   1,
	}
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

func (g *Get) toProto() pb.Message {
	get := &proto.Get{
		Row: g.key,
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

func (c *Client) Get(table string, get *Get) (*ResultRow, error) {
	log.Info("start get")
	ch := c.action([]byte(table), get.key, get, true, 0)

	response := <-ch
	switch r := response.(type) {
	case *proto.GetResponse:
		return newResultRow(r.GetResult()), nil
	}

	return nil, fmt.Errorf("No valid response seen [response: %#v]", response)
}
