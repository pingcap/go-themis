package themis

import (
	pb "github.com/golang/protobuf/proto"
	"github.com/ngaut/log"
	"github.com/pingcap/go-themis/proto"
)

type action interface {
	toProto() pb.Message
}

func (c *Client) action(table, row []byte, action action, useCache bool, retries int) chan pb.Message {
	region := c.locateRegion(table, row, useCache)
	conn := c.getConn(region.Server, false)
	if conn == nil || region == nil {
		return nil
	}

	regionSpecifier := &proto.RegionSpecifier{
		Type:  proto.RegionSpecifier_REGION_NAME.Enum(),
		Value: []byte(region.Name),
	}
	log.Info(region.Name)

	var cl *call = nil
	switch a := action.(type) {
	case *Get:
		cl = newCall(&proto.GetRequest{
			Region: regionSpecifier,
			Get:    a.toProto().(*proto.Get),
		})
	case *Put:
		cl = newCall(&proto.MutateRequest{
			Region:   regionSpecifier,
			Mutation: a.toProto().(*proto.MutationProto),
		})
	}

	result := make(chan pb.Message)

	go func() {
		r := <-cl.responseCh

		switch r.(type) {
		case *exception:
			if retries <= c.maxRetries {
				// retry action, and refresh region info
				log.Infof("Retrying action for the %d time", retries+1)
				newr := c.action(table, row, action, false, retries+1)
				result <- <-newr
			} else {
				result <- r
			}
			return
		default:
			result <- r
		}
	}()

	if cl != nil {
		err := conn.call(cl)

		if err != nil {
			log.Warningf("Error return while attempting call [err=%#v]", err)
			// purge dead server
			delete(c.cachedConns, region.Server)

			if retries <= c.maxRetries {
				// retry action
				log.Infof("Retrying action for the %d time", retries+1)
				c.action(table, row, action, false, retries+1)
			}
		}
	}

	return result
}
