package themis

import "errors"

type ThemisGet struct {
	get *Get
}

var ErrContainPreservedColumn = errors.New("contain preserved column")

func NewThemisGet(g *Get) *ThemisGet {
	return &ThemisGet{g}
}

func (g *ThemisGet) AddColumn(family, qual []byte) error {
	if isContainPreservedColumn(family, qual) {
		return ErrContainPreservedColumn
	}
	g.get.AddColumn(family, qual)
	return nil
}

func (g *ThemisGet) AddFamily(family []byte) error {
	if isContainPreservedColumn(family, nil) {
		return ErrContainPreservedColumn
	}
	g.get.AddFamily(family)
	return nil
}
