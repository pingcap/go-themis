package oracles

import (
	"github.com/juju/errors"
	"github.com/ngaut/tso/client"
)

const remoteRetryTimes = 3

// RemoteOracle is an oracle that use a remote data source.
// Refer https://github.com/ngaut/tso for more details.
type RemoteOracle struct {
	c *client.Client
}

// NewRemoteOracle creates a RemoteOracle, addr should be formatted as 'host:port'.
func NewRemoteOracle(addr string) *RemoteOracle {
	return &RemoteOracle{
		c: client.NewClient(&client.Conf{ServerAddr: addr}),
	}
}

// GetTimestamp gets timestamp from remote data source.
func (t *RemoteOracle) GetTimestamp() (uint64, error) {
	var err error
	for i := 0; i < remoteRetryTimes; i++ {
		ts, e := t.c.GoGetTimestamp().GetTS()
		if e == nil {
			return uint64((ts.Physical << 18) + ts.Logical), nil
		}
		err = errors.Trace(e)
	}
	return 0, err
}
