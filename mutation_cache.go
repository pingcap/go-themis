package themis

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

type mutationValuePair struct {
	typ   Type
	value []byte
}

type columnMutation struct {
	*column
	*mutationValuePair
}

type rowMutation struct {
	row []byte
	// mutations := { 'cf:col' => mutationValuePair }
	mutations map[string]*mutationValuePair
}

func newRowMutation(row []byte) *rowMutation {
	return &rowMutation{
		row:       row,
		mutations: map[string]*mutationValuePair{},
	}
}

func (r *rowMutation) addMutation(c *column, typ Type, val []byte) {
	r.mutations[c.String()] = &mutationValuePair{
		typ:   typ,
		value: val,
	}
}

func (r *rowMutation) mutationList() []*columnMutation {
	var ret []*columnMutation
	for k, v := range r.mutations {
		ret = append(ret, &columnMutation{
			column:            columnFromString(k),
			mutationValuePair: v,
		})
	}
	return ret
}

type columnMutationCache struct {
	// mutations => {table => { rowKey => row mutations } }
	mutations map[string]map[string]*rowMutation
}

func newColumnMutationCache() *columnMutationCache {
	return &columnMutationCache{
		mutations: map[string]map[string]*rowMutation{},
	}
}

func (c *columnMutationCache) addMutation(tbl []byte, row []byte, col *column, t Type, v []byte) {
	tblRowMutations, ok := c.mutations[string(tbl)]
	if !ok {
		// create table mutation map
		tblRowMutations = map[string]*rowMutation{}
		c.mutations[string(tbl)] = tblRowMutations
	}

	rowMutations, ok := tblRowMutations[string(row)]
	if !ok {
		// create row mutation map
		rowMutations = newRowMutation(row)
		tblRowMutations[string(row)] = rowMutations
	}
	rowMutations.addMutation(col, t, v)
}

func (c *columnMutationCache) getMutation(cc *columnCoordinate) *mutationValuePair {
	t, ok := c.mutations[string(cc.table)]
	if !ok {
		return nil
	}
	rowMutation, ok := t[string(cc.row)]
	if !ok {
		return nil
	}
	p, ok := rowMutation.mutations[cc.getColumn().String()]
	if !ok {
		return nil
	}
	return p
}
