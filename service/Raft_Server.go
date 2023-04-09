package service

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"strconv"
	"sync"
	"time"
)

type ServiceType uint64
type Server struct {
	mu         sync.Mutex
	serverId   uint64
	peerList   Set
	rpcServer  *rpc.Server
	listener   net.Listener
	peers      map[uint64]*rpc.Client
	quit       chan interface{}
	wg         sync.WaitGroup
	rn         *RaftNode
	db         *Database
	rpcProxy   *RPCProxy
	commitChan chan CommitEntry
	ready      chan interface{}
	service    *ServiceType
}

type RPCProxy struct {
	rn *RaftNode
}

// create a Server Instance with serverId and list of peerIds
func CreateServer(serverId uint64, listner net.Listener) *Server {
	s := new(Server)
	ready := make(chan interface{})
	s.serverId = serverId
	s.ready = ready
	peerList := makeSet()
	s.peers = make(map[uint64]*rpc.Client)
	for i := uint64(0); i < 5; i++ {
		if i+1 == serverId {
			continue
		} else {
			peerList.Add(i + 1)
		}
	}
	s.peerList = peerList
	serverAddress := "localhost:"
	for j := uint64(0); j < 5; j++ {
		if j+1 == serverId {
			continue
		} else {
			port := int(12000 + j + 1)
			address := serverAddress + strconv.Itoa(port)
			addr, _ := net.ResolveTCPAddr("tcp", address)
			go s.ConnectToPeer(j+1, addr)
		}
	}
	s.listener = listner
	s.db = NewDatabase()
	s.commitChan = make(chan CommitEntry)
	s.quit = make(chan interface{})
	return s
}

// keep listening for incoming connections in a loop
// on accepting a connection start a go routine to serve the connection
func (s *Server) ConnectionAccept() {
	defer s.wg.Done()
	for {
		listener, err := s.listener.Accept()
		if err != nil {
			select {
			case <-s.quit:
				log.Printf("[%d] Accepting no more connections\n", s.serverId)
				return
			default:
				log.Fatalf("[%d] Error in accepting %v\n", s.serverId, err)
			}
		}
		s.wg.Add(1)
		go func() {
			s.rpcServer.ServeConn(listener)
			s.wg.Done()
		}()
	}
}

// start a new service ->
// 1. create the RPC Server
// 2. register the service with RPC
// 3. get a lister for TCP port passed as argument
// 4. start listening for incoming connections
func (s *Server) Serve() {
	s.mu.Lock()
	close(s.ready)
	s.rn = NewRaftNode(s.serverId, s.peerList, s, s.db, s.ready, s.commitChan)
	s.rpcServer = rpc.NewServer()
	s.rpcProxy = &RPCProxy{rn: s.rn}
	s.rpcServer.RegisterName("RaftNode", s.rpcProxy)

	var st ServiceType = ServiceType(1)
	s.service = &st
	s.rpcServer.RegisterName("ServiceType", s.service)
	s.mu.Unlock()
	s.wg.Add(1)
	go s.ConnectionAccept()
}

// close connections to all peers
func (s *Server) DisconnectAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.peers {
		if s.peers[id] != nil {
			s.peers[id].Close()
			s.peers[id] = nil
		}
	}
}

// stop the server
func (s *Server) Stop() {
	s.rn.Stop()
	close(s.quit)
	s.listener.Close()

	log.Printf("[%d] Waiting for existing connections to close\n", s.serverId)
	s.wg.Wait()

	log.Printf("[%d] All connections closed. Stopping server\n", s.serverId)
}

func (s *Server) GetListenerAddr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listener.Addr()
}

// connect to a peer
func (s *Server) ConnectToPeer(peerId uint64, addr net.Addr) error {

	for {
		s.mu.Lock()
		if s.peers[peerId] == nil {
			peer, err := rpc.Dial(addr.Network(), addr.String())
			if err != nil {
				log.Println(err)
				//return err
			}
			s.peers[peerId] = peer
			log.Println(s.peers[peerId])
		}
		s.mu.Unlock()
	}
	return nil
}

// disconnect from a particular peer
func (s *Server) DisconnectPeer(peerId uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	peer := s.peers[peerId]
	if peer != nil {
		err := peer.Close()
		s.peers[peerId] = nil
		return err
	}
	return nil
}

// make an RPC call to the particular peer
func (s *Server) RPC(peerId uint64, rpcCall string, args interface{}, reply interface{}) error {
	s.mu.Lock()
	peer := s.peers[peerId] //obtain the peer client
	s.mu.Unlock()

	if peer == nil {
		return fmt.Errorf("[%d] RPC call to peer %d after it is closed", s.serverId, peerId)
	} else {
		// call RPC corresponding to the particular peer connection
		return peer.Call(rpcCall, args, reply)
	}
}

// A DUMMY RPC FUNCTION
func (s *ServiceType) DisplayMsg(args uint64, reply *uint64) error {
	fmt.Printf("received %d\n", args)
	*reply = 2 * args
	return nil
}

// RPC call from proxy for RequestVote
func (rp *RPCProxy) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	return rp.rn.RequestVote(args, reply)
}

// RPC call from proxy for AppendEntries
func (rp *RPCProxy) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	time.Sleep(time.Duration(1+rand.Intn(5)) * time.Millisecond)
	return rp.rn.AppendEntries(args, reply)
}
