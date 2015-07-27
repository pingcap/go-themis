package themis

import (
	"fmt"

	pb "github.com/golang/protobuf/proto"
	"github.com/pingcap/go-themis/proto"

	"bytes"
	"encoding/binary"
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

type Client struct {
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

func NewClient(zkHosts []string, zkRoot string) (*Client, error) {
	cl := &Client{
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
func (c *Client) init() error {
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

func (c *Client) decodeMeta(data []byte) (*proto.ServerName, error) {
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

func (c *Client) getConn(addr string, isMaster bool) *connection {
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

// TODO parse region info
