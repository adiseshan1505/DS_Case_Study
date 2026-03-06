package main

import (
	"bufio"
	"ds_case_study/common"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type ClientNode struct {
	ID         int
	Addr       string
	ServerAddr string
	Peers      []common.ClientInfo
	LeaderID   int
	IsLeader   bool
	mu         sync.Mutex

	// For 2PC state
	TxState map[string]string // txID -> state (Prepared, Committed, Aborted)
}

func NewClientNode(addr, serverAddr string) *ClientNode {
	return &ClientNode{
		Addr:       addr,
		ServerAddr: serverAddr,
		LeaderID:   -1,
		TxState:    make(map[string]string),
	}
}

// ------------------- RPC METHODS FOR PEER-TO-PEER (BULLY ALGORITHM) -------------------

// ElectionMsg handles incoming Bully election messages
func (n *ClientNode) ElectionMsg(senderID int, ok *bool) error {
	fmt.Printf("[ELECTION] Received ELECTION message from ID: %d\n", senderID)
	// We only reply OK if our ID is higher
	if n.ID > senderID {
		*ok = true
	} else {
		*ok = false
	}
	return nil
}

// CoordinatorMsg handles the message when someone declares themselves Leader
func (n *ClientNode) CoordinatorMsg(leaderID int, ok *bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.LeaderID = leaderID
	n.IsLeader = (leaderID == n.ID)
	*ok = true
	fmt.Printf("[ELECTION] Node %d has declared itself the new LEADER!\n", leaderID)
	return nil
}

// ------------------- RPC METHODS FOR 2-PHASE COMMIT (PARTICIPANT) -------------------

func (n *ClientNode) Prepare(args *common.PrepareArgs, vote *bool) error {
	fmt.Printf("[2PC] Received PREPARE for Tx: %s with Data: %s\n", args.TxID, args.Data)
	n.mu.Lock()
	defer n.mu.Unlock()
	
	// Simulate "writing to WAL" or checking constraints
	// Let's assume this participant agrees to commit
	n.TxState[args.TxID] = "PREPARED"
	*vote = true
	fmt.Printf("[2PC] Voted YES for Tx: %s\n", args.TxID)
	return nil
}

func (n *ClientNode) Commit(txID string, ok *bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.TxState[txID] = "COMMITTED"
	*ok = true
	fmt.Printf("[2PC] COMMITTED Tx: %s successfully! Spanner Database Updated.\n", txID)
	return nil
}

func (n *ClientNode) Abort(txID string, ok *bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.TxState[txID] = "ABORTED"
	*ok = true
	fmt.Printf("[2PC] ABORTED Tx: %s. State rolled back.\n", txID)
	return nil
}

// ------------------- CORE ACTIONS -------------------

func (n *ClientNode) RegisterWithServer() {
	client, err := rpc.Dial("tcp", n.ServerAddr)
	if err != nil {
		log.Fatalf("Failed to connect to Central Server: %v", err)
	}
	defer client.Close()

	args := common.RegisterArgs{Addr: n.Addr}
	var reply common.RegisterReply
	err = client.Call("CentralServer.RegisterClient", &args, &reply)
	if err != nil {
		log.Fatalf("Register RPC failed: %v", err)
	}

	n.ID = reply.ID
	fmt.Printf("[INIT] Registered with Central Server. My Client ID is: %d\n", n.ID)
}

func (n *ClientNode) FetchPeers() {
	client, err := rpc.Dial("tcp", n.ServerAddr)
	if err != nil {
		fmt.Printf("Error connecting to server for GetClients: %v\n", err)
		return
	}
	defer client.Close()

	var clients []common.ClientInfo
	err = client.Call("CentralServer.GetClients", 0, &clients)
	if err == nil {
		n.mu.Lock()
		n.Peers = clients
		n.mu.Unlock()
		fmt.Printf("[DISCOVERY] Fetched %d nodes from the network.\n", len(clients))
	} else {
		fmt.Printf("GetClients RPC failed: %v\n", err)
	}
}

// RunElection executes the Bully Algorithm
func (n *ClientNode) RunElection() {
	fmt.Println("\n[ELECTION] Starting Bully Leader Election...")
	n.FetchPeers()


	for _, peer := range n.Peers {
		if peer.ID > n.ID {

			peerClient, err := rpc.Dial("tcp", peer.Addr)
			if err != nil {
				continue // Peer might be down
			}

			var ok bool
			err = peerClient.Call("ClientNode.ElectionMsg", n.ID, &ok)
			if err == nil && ok {
				// A higher node responded, we back down
				fmt.Printf("[ELECTION] Higher node %d responded to election. I yield.\n", peer.ID)
				peerClient.Close()
				return
			}
			peerClient.Close()
		}
	}

	// No higher nodes responded. I am the Leader!
	fmt.Println("[ELECTION] No higher nodes responded or none exist. I AM THE LEADER!")
	n.mu.Lock()
	n.IsLeader = true
	n.LeaderID = n.ID
	n.mu.Unlock()

	// Broadcast Coordinator message
	for _, peer := range n.Peers {
		if peer.ID != n.ID {
			peerClient, err := rpc.Dial("tcp", peer.Addr)
			if err == nil {
				var ok bool
				peerClient.Call("ClientNode.CoordinatorMsg", n.ID, &ok)
				peerClient.Close()
			}
		}
	}
	
	// Since I am leader, I will acquire Lock and do a 2PC transaction!
	go n.ExecuteTransactionAsLeader()
}

// Mutual Exclusion & 2-Phase Commit
func (n *ClientNode) ExecuteTransactionAsLeader() {
	time.Sleep(2 * time.Second) // wait for everyone to realize I'm leader

	// 1. MUTUAL EXCLUSION
	serverClient, err := rpc.Dial("tcp", n.ServerAddr)
	if err != nil {
		fmt.Println("[LOCK] Error connecting to server for lock")
		return
	}
	var lockGranted bool
	err = serverClient.Call("CentralServer.RequestLock", n.ID, &lockGranted)
	if err != nil || !lockGranted {
		fmt.Println("[LOCK] Failed to acquire Distributed Lock. Cannot start transaction.")
		serverClient.Close()
		return
	}
	fmt.Println("[LOCK] Acquired Distributed Lock! Mutual Exclusion guaranteed. Proceeding with 2PC.")

	// 2. TWO-PHASE COMMIT (Prepare Phase)
	txID := fmt.Sprintf("TXN-%d", time.Now().Unix())
	data := "Update Spanner Balance = $1000"
	fmt.Printf("\n[2PC-LEADER] Starting TWO-PHASE COMMIT for Tx %s...\n", txID)

	allVotedYes := true
	n.FetchPeers() // Get latest

	for _, peer := range n.Peers {
		if peer.ID == n.ID { continue }
		fmt.Printf("[2PC-LEADER] Sending PREPARE to Node %d...\n", peer.ID)
		peerClient, err := rpc.Dial("tcp", peer.Addr)
		if err != nil {
			fmt.Printf("[2PC-LEADER] Node %d unreachable! 2PC failing.\n", peer.ID)
			allVotedYes = false
			break
		}
		
		var vote bool
		err = peerClient.Call("ClientNode.Prepare", &common.PrepareArgs{TxID: txID, Data: data}, &vote)
		if err != nil || !vote {
			fmt.Printf("[2PC-LEADER] Node %d voted NO or failed: %v\n", peer.ID, err)
			allVotedYes = false
			peerClient.Close()
			break
		}
		fmt.Printf("[2PC-LEADER] Node %d voted YES.\n", peer.ID)
		peerClient.Close()
	}

	// 3. TWO-PHASE COMMIT (Commit Phase)
	if allVotedYes {
		fmt.Printf("[2PC-LEADER] All participants voted YES. Sending COMMIT Phase...\n")
		for _, peer := range n.Peers {
            if peer.ID == n.ID { continue }
			peerClient, _ := rpc.Dial("tcp", peer.Addr)
			if peerClient != nil {
				var ok bool
				peerClient.Call("ClientNode.Commit", txID, &ok)
				peerClient.Close()
			}
		}
		fmt.Printf("[2PC-LEADER] Transaction %s Global Commit Successful!\n", txID)
	} else {
		fmt.Printf("[2PC-LEADER] Missing votes. Sending ABORT Phase...\n")
		for _, peer := range n.Peers {
            if peer.ID == n.ID { continue }
			peerClient, _ := rpc.Dial("tcp", peer.Addr)
			if peerClient != nil {
				var ok bool
				peerClient.Call("ClientNode.Abort", txID, &ok)
				peerClient.Close()
			}
		}
		fmt.Printf("[2PC-LEADER] Transaction %s Global Abort complete.\n", txID)
	}

	// 4. RELEASE LOCK
	var releaseOk bool
	serverClient.Call("CentralServer.ReleaseLock", n.ID, &releaseOk)
	serverClient.Close()
	fmt.Println("[LOCK] Released Distributed Lock.\n")
}

func main() {
	serverHost := flag.String("server", "127.0.0.1:8080", "Server Address")
	portStr := flag.String("port", "9001", "My Client RPC Port")
	flag.Parse()

	addr := "127.0.0.1:" + *portStr
	node := NewClientNode(addr, *serverHost)

	// Start P2P RPC Listening
	rpc.Register(node)
	listener, err := net.Listen("tcp", "0.0.0.0:"+*portStr)
	if err != nil {
		log.Fatalf("Client Listen error: %v", err)
	}

	go func() {
		rpc.Accept(listener)
	}()

	fmt.Printf("Started Laptop/Client Node on port %s.\n", *portStr)
	
	// Connect to Central Server
	node.RegisterWithServer()

	// Wait for user manual trigger to run election (simulates someone starting a transaction)
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Println("\nPress 'e' followed by Enter to trigger an Election, or 'q' to quit.")
		input, _ := reader.ReadString('\n')
		if len(input) > 0 && input[0] == 'e' {
			node.RunElection()
		} else if len(input) > 0 && input[0] == 'q' {
			break
		}
	}
}