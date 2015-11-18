package oracles

import "github.com/ngaut/tso/client"

type TsoOracle struct {
	c *client.Client
}

func NewTsoOracle(addr string) *TsoOracle {
	return &TsoOracle{
		c: client.NewClient(&client.Conf{ServerAddr: addr}),
	}
}

func (t *TsoOracle) GetTimestamp() (uint64, error) {
	ts, err := t.c.GoGetTimestamp().GetTS()
	if err != nil {
		return 0, err
	}
	return uint64((ts.Physical << 18) + ts.Logical), err
}
