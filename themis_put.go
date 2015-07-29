package themis

type ThemisPut struct {
	put *Put
}

func NewThemisPut(p *Put) *ThemisPut {
	return &ThemisPut{p}
}

func (g *ThemisPut) AddValue(family, qual, value []byte) error {
	if isContainPreservedColumn(family, qual) {
		return ErrContainPreservedColumn
	}
	g.put.AddValue(family, qual, value)
	return nil
}
