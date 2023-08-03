package dso

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"

	"imdea.org/redbelly/logging"
	"imdea.org/redbelly/types"
)

type DSOServer struct {
	dso *DSO

	logger *logging.Logger
}

func NewDSOServer(dso *DSO) *DSOServer {
	return &DSOServer{
		dso:    dso,
		logger: logging.NewLogger("DSOServer"),
	}
}

func (s *DSOServer) Add(e types.Transaction, ok *bool) error {
	s.dso.Add(e)
	*ok = true
	return nil
}

func (s *DSOServer) Get(noinput struct{}, set *types.TransactionSet) error {
	*set = s.dso.Get()
	return nil
}

func (s *DSOServer) Start(port int) error {
	rpc.Register(s)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if e != nil {
		return e
	}
	go http.Serve(l, nil)
	s.logger.Info.Printf("DSO Server listening at %d\n", port)
	return nil
}

type DSOClient struct {
	httpClient *rpc.Client

	logger *logging.Logger
}

func NewDSOClient() *DSOClient {
	return &DSOClient{
		logger: logging.NewLogger("DSOClient"),
	}
}

func (c *DSOClient) Connect(serverAddress string) error {
	var e error
	c.httpClient, e = rpc.DialHTTP("tcp", serverAddress)
	return e
}

func (c *DSOClient) Add(e types.Transaction) error {
	var ok bool
	err := c.httpClient.Call("DSOServer.Add", e, &ok)
	if err != nil {
		return err
	}
	return nil
}

func (c *DSOClient) Get() (types.TransactionSet, error) {
	var set types.TransactionSet
	err := c.httpClient.Call("DSOServer.Get", struct{}{}, &set)
	if err != nil {
		return nil, err
	}
	return set, nil
}

type WDSOClient struct {
	httpClients []*rpc.Client
	f           int

	logger *logging.Logger
}

func NewWDSOClient(f int) *WDSOClient {
	return &WDSOClient{
		f: f,

		logger: logging.NewLogger("DSOClient"),
	}
}

func (c *WDSOClient) Connect(servers []string) error {
	if c.httpClients == nil {
		c.httpClients = make([]*rpc.Client, len(servers))
	}
	var e error
	for i, s := range servers {
		if c.httpClients[i] == nil {
			c.httpClients[i], e = rpc.DialHTTP("tcp", s)
			if e != nil {
				return fmt.Errorf("Error on server %s: %w", s, e)
			}
		}
	}
	return nil
}

func (c *WDSOClient) Add(e types.Transaction) error {
	var ok bool
	errs := make(chan error, len(c.httpClients))
	for i := 0; i < 2*c.f+1; i++ {
		go func(index int) {
			errs <- c.httpClients[index].Call("DSOServer.Add", e, &ok)
		}(i)
	}
	var err error
	for i := 0; i < c.f+1; i++ {
		err = <-errs
		if err != nil {
			return fmt.Errorf("Error on add: %w", err)
		}
	}
	return nil
}

//TODO Manage errors, shuffle servers
func (c *WDSOClient) Get() (types.TransactionSet, error) {
	responses := make(chan types.TransactionSet, len(c.httpClients))
	for i := 0; i < 3*c.f+1; i++ {
		go func(index int) {
			var set types.TransactionSet
			err := c.httpClients[index].Call("DSOServer.Get", struct{}{}, &set)
			if err == nil {
				responses <- set
			} else {
				c.logger.Error.Println("Failed Get request to server", index)
			}
		}(i)
	}
	voted := make(map[string]uint)
	for i := 0; i < 2*c.f+1; i++ {
		set := <-responses
		for transaction := range set {
			voted[transaction]++
		}
	}
	resp := make(types.TransactionSet)
	quorum := uint(c.f + 1)
	for tx, votes := range voted {
		if votes >= quorum {
			resp[tx] = true
		}
	}
	return resp, nil
}
