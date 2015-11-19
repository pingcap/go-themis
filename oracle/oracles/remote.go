package oracles

import "github.com/ngaut/tso/client"

type RemoteOracle struct {
	c *client.Client
}

func NewRemoteOracle(addr string) *RemoteOracle {
	return &RemoteOracle{
		c: client.NewClient(&client.Conf{ServerAddr: addr}),
	}
}

func (t *RemoteOracle) GetTimestamp() (uint64, error) {
	ts, err := t.c.GoGetTimestamp().GetTS()
	if err != nil {
		return 0, err
	}
	return uint64((ts.Physical << 18) + ts.Logical), err
}
