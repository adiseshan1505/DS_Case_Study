package main

import (
	"ds_case_study/common"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
)

type CentralServer struct {
	mu           sync.Mutex
	clients      map[int]common.ClientInfo
	nextID       int
	lockOwner    int
	isLocked     bool
}

// RegisterClient is called by new laptops joining the cluster
func (s *CentralServer) RegisterClient(args *common.RegisterArgs, reply *common.RegisterReply) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := s.nextID
	s.nextID++

	s.clients[id] = common.ClientInfo{
		ID:   id,
		Addr: args.Addr,
	}
	reply.ID = id
	fmt.Printf("[SERVER] Registered new client ID: %d at %s\n", id, args.Addr)
	return nil
}

// GetClients returns all registered laptops so they can communicate P2P
func (s *CentralServer) GetClients(_ int, reply *[]common.ClientInfo) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, c := range s.clients {
		*reply = append(*reply, c)
	}
	return nil
}

// RequestLock for Centralized Mutual Exclusion
func (s *CentralServer) RequestLock(clientID int, success *bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isLocked {
		s.isLocked = true
		s.lockOwner = clientID
		*success = true
		fmt.Printf("[SERVER] Lock granted to Client ID: %d\n", clientID)
	} else {
		*success = false
		fmt.Printf("[SERVER] Lock denied to Client ID: %d (Currently held by %d)\n", clientID, s.lockOwner)
	}
	return nil
}

// ReleaseLock for Mutual Exclusion
func (s *CentralServer) ReleaseLock(clientID int, success *bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isLocked && s.lockOwner == clientID {
		s.isLocked = false
		s.lockOwner = -1
		*success = true
		fmt.Printf("[SERVER] Lock released by Client ID: %d\n", clientID)
	} else {
		*success = false
	}
	return nil
}

func main() {
	port := flag.String("port", "8080", "Port to listen on")
	flag.Parse()

	server := &CentralServer{
		clients:   make(map[int]common.ClientInfo),
		nextID:    1,
		lockOwner: -1,
	}

	rpc.Register(server)
	listener, err := net.Listen("tcp", "0.0.0.0:"+*port)
	if err != nil {
		log.Fatalf("Server Listen error: %v", err)
	}

	fmt.Printf("Central Server started on port %s...\n", *port)
	fmt.Println("Waiting for laptops (clients) to register...")
	rpc.Accept(listener)
}