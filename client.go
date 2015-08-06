package themis

import (
	"crypto/md5"
	"fmt"
	"strings"

	pb "github.com/golang/protobuf/proto"
	"github.com/pingcap/go-themis/hbase"
	"github.com/pingcap/go-themis/proto"

	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"time"

	"github.com/ngaut/go-zookeeper/zk"
	"github.com/ngaut/log"
)

const (
	zkRootRegionPath = "/meta-region-server"
	zkMasterAddrPath = "/master"

	magicHeadByte            = 0xff
	magicHeadSize            = 1
	idLengthSize             = 4
	md5HexSize               = 32
	servernameSeparator      = ","
	rpcTimeout               = 30000
	pingTimeout              = 30000
	callTimeout              = 5000
	defaultMaxActionRetries  = 3
	socketDefaultRetryWaitMs = 200
)

var (
	hbaseHeaderBytes []byte = []byte("HBas")
	metaTableName    []byte = []byte("hbase:meta")
	metaRegionName   []byte = []byte("hbase:meta,,1")
)

type RegionInfo struct {
	Server         string
	StartKey       []byte
	EndKey         []byte
	Name           string
	Ts             string
	TableNamespace string
	TableName      string
}

type TableInfo struct {
	TableName string
	Families  []string
}

type hbaseClient interface {
	Get(tbl string, g *hbase.Get) (*hbase.ResultRow, error)
	Put(tbl string, p *hbase.Put) (bool, error)
	Delete(tbl string, d *hbase.Delete) (bool, error)
	ServiceCall(table string, call *hbase.CoprocessorServiceCall) (*proto.CoprocessorServiceResponse, error)
}

var _ hbaseClient = (*client)(nil)

type client struct {
	zkClient         *zk.Conn
	zkHosts          []string
	zkRoot           string
	cachedConns      map[string]*connection
	cachedRegionInfo map[string]map[string]*RegionInfo
	maxRetries       int
	prefetched       map[string]bool
	rootServerName   *proto.ServerName
	masterServerName *proto.ServerName
}

func serverNameToAddr(server *proto.ServerName) string {
	return fmt.Sprintf("%s:%d", server.GetHostName(), server.GetPort())
}

func NewClient(zkHosts []string, zkRoot string) (*client, error) {
	cl := &client{
		zkHosts:          zkHosts,
		zkRoot:           zkRoot,
		cachedConns:      make(map[string]*connection),
		cachedRegionInfo: make(map[string]map[string]*RegionInfo),
		prefetched:       make(map[string]bool),
		maxRetries:       defaultMaxActionRetries,
	}
	err := cl.init()
	if err != nil {
		return nil, err
	}
	return cl, nil
}

// init and get root region server addr and master addr
func (c *client) init() error {
	zkclient, _, err := zk.Connect(c.zkHosts, time.Second*30)
	if err != nil {
		return err
	}
	c.zkClient = zkclient

	res, _, _, err := c.zkClient.GetW(c.zkRoot + zkRootRegionPath)
	if err != nil {
		return err
	}
	c.rootServerName, err = c.decodeMeta(res)
	if err != nil {
		return err
	}
	log.Info("connect root region server...")
	conn, err := newConnection(serverNameToAddr(c.rootServerName), false)
	if err != nil {
		return err
	}
	// set buffered regionserver conn
	c.cachedConns[serverNameToAddr(c.rootServerName)] = conn

	res, _, _, err = c.zkClient.GetW(c.zkRoot + zkMasterAddrPath)
	if err != nil {
		return err
	}
	c.masterServerName, err = c.decodeMeta(res)
	if err != nil {
		return err
	}
	return nil
}

func (c *client) decodeMeta(data []byte) (*proto.ServerName, error) {
	if data[0] != magicHeadByte {
		return nil, errors.New("unknown packet")
	}

	var n int32
	binary.Read(bytes.NewBuffer(data[1:]), binary.BigEndian, &n)
	dataOffset := magicHeadSize + idLengthSize + int(n)
	data = data[(dataOffset + 4):]

	var mrs proto.MetaRegionServer
	err := pb.Unmarshal(data, &mrs)
	if err != nil {
		return nil, err
	}

	return mrs.GetServer(), nil
}

func (c *client) getConn(addr string, isMaster bool) *connection {
	if s, ok := c.cachedConns[addr]; ok {
		return s
	}
	conn, err := newConnection(addr, isMaster)
	if err != nil {
		log.Error(err)
		return nil
	}
	c.cachedConns[addr] = conn
	return conn
}

// http://stackoverflow.com/questions/27602013/correct-way-to-get-region-name-by-using-hbase-api
func (c *client) createRegionName(table, startKey []byte, id string, newFormat bool) []byte {
	if len(startKey) == 0 {
		startKey = make([]byte, 1)
	}

	b := bytes.Join([][]byte{table, startKey, []byte(id)}, []byte(","))

	if newFormat {
		m := md5.Sum(b)
		mhex := []byte(hex.EncodeToString(m[:]))
		b = append(bytes.Join([][]byte{b, mhex}, []byte(".")), []byte(".")...)
	}
	return b
}

func (c *client) parseRegion(rr *hbase.ResultRow) *RegionInfo {
	if regionInfoCol, ok := rr.Columns["info:regioninfo"]; ok {
		offset := strings.Index(string(regionInfoCol.Value), "PBUF") + 4
		regionInfoBytes := regionInfoCol.Value[offset:]

		var info proto.RegionInfo
		err := pb.Unmarshal(regionInfoBytes, &info)

		if err != nil {
			log.Errorf("Unable to parse region location: %#v", err)
		}

		log.Debugf("Parsed region info [name=%s]", rr.Row)

		return &RegionInfo{
			Server:         string(rr.Columns["info:server"].Value),
			StartKey:       info.GetStartKey(),
			EndKey:         info.GetEndKey(),
			Name:           string(rr.Row),
			TableNamespace: string(info.GetTableName().GetNamespace()),
			TableName:      string(info.GetTableName().GetQualifier()),
		}
	}

	log.Errorf("Unable to parse region location (no regioninfo column): %#v", rr)

	return nil
}

func (c *client) cacheLocation(table []byte, region *RegionInfo) {
	tableStr := string(table)
	if _, ok := c.cachedRegionInfo[tableStr]; !ok {
		c.cachedRegionInfo[tableStr] = make(map[string]*RegionInfo)
	}
	c.cachedRegionInfo[tableStr][region.Name] = region
}

func (c *client) prefetchRegionCache(table []byte) {
	if bytes.Equal(table, metaTableName) {
		return
	}

	if v, ok := c.prefetched[string(table)]; ok && v {
		return
	}

	startRow := table
	stopRow := incrementByteString(table, len(table)-1)

	scan := newScan(metaTableName, c)

	scan.StartRow = startRow
	scan.StopRow = stopRow

	for {
		r := scan.Next()
		if r == nil || scan.closed {
			break
		}
		region := c.parseRegion(r)
		if region != nil {
			c.cacheLocation(table, region)
		}
	}

	c.prefetched[string(table)] = true
}

func (c *client) getCachedLocation(table, row []byte) *RegionInfo {
	tableStr := string(table)
	if regions, ok := c.cachedRegionInfo[tableStr]; ok {
		for _, region := range regions {
			if (len(region.EndKey) == 0 ||
				bytes.Compare(row, region.EndKey) < 0) &&
				(len(region.StartKey) == 0 ||
					bytes.Compare(row, region.StartKey) >= 0) {
				return region
			}
		}
	}
	return nil
}

func (c *client) locateRegion(table, row []byte, useCache bool) *RegionInfo {
	metaRegion := &RegionInfo{
		StartKey: []byte{},
		EndKey:   []byte{},
		Name:     string(metaRegionName),
		Server:   serverNameToAddr(c.rootServerName),
	}

	if bytes.Equal(table, metaTableName) {
		return metaRegion
	}

	c.prefetchRegionCache(table)
	if r := c.getCachedLocation(table, row); r != nil && useCache {
		return r
	}

	conn := c.getConn(metaRegion.Server, false)

	regionRow := c.createRegionName(table, row, "", true)

	call := newCall(&proto.GetRequest{
		Region: &proto.RegionSpecifier{
			Type:  proto.RegionSpecifier_REGION_NAME.Enum(),
			Value: metaRegionName,
		},
		Get: &proto.Get{
			Row: regionRow,
			Column: []*proto.Column{&proto.Column{
				Family: []byte("info"),
			}},
			ClosestRowBefore: pb.Bool(true),
		},
	})

	conn.call(call)

	response := <-call.responseCh

	switch r := response.(type) {
	case *proto.GetResponse:
		rr := hbase.NewResultRow(r.GetResult())
		if region := c.parseRegion(rr); region != nil {
			log.Debugf("Found region [region: %s]", region.Name)
			// update cache
			c.cacheLocation(table, region)
			return region
		}
	}

	log.Debugf("Couldn't find the region: [table=%s] [row=%s] [region_row=%s]", table, row, regionRow)

	return nil
}

func (c *client) Delete(table string, del *hbase.Delete) (bool, error) {
	ch := c.action([]byte(table), del.GetRow(), del, true, 0)

	response := <-ch
	switch r := response.(type) {
	case *proto.MutateResponse:
		return r.GetProcessed(), nil
	}

	return false, fmt.Errorf("No valid response seen [response: %#v]", response)
}

func (c *client) Get(table string, get *hbase.Get) (*hbase.ResultRow, error) {
	ch := c.action([]byte(table), get.GetRow(), get, true, 0)
	if ch == nil {
		return nil, fmt.Errorf("Create region server connection failed")
	}

	response := <-ch
	switch r := response.(type) {
	case *proto.GetResponse:
		return hbase.NewResultRow(r.GetResult()), nil
	case *exception:
		return nil, errors.New(r.msg)
	}
	return nil, fmt.Errorf("No valid response seen [response: %#v]", response)
}

func (c *client) Put(table string, put *hbase.Put) (bool, error) {
	ch := c.action([]byte(table), put.GetRow(), put, true, 0)

	response := <-ch
	switch r := response.(type) {
	case *proto.MutateResponse:
		return r.GetProcessed(), nil
	}

	return false, fmt.Errorf("No valid response seen [response: %#v]", response)
}

func (c *client) ServiceCall(table string, call *hbase.CoprocessorServiceCall) (*proto.CoprocessorServiceResponse, error) {
	ch := c.action([]byte(table), call.Row, call, true, 0)
	response := <-ch
	switch r := response.(type) {
	case *proto.CoprocessorServiceResponse:
		return r, nil
	case *exception:
		log.Error(r.msg)
		return nil, errors.New(r.msg)
	}

	return nil, fmt.Errorf("No valid response seen [response: %#v]", response)
}
