package common

// ClientInfo holds the metadata for each registered laptop/client
type ClientInfo struct {
	ID   int    // Unique ID assigned by the server
	Addr string // IP:Port for this client's RPC server
}

type RegisterArgs struct {
	Addr string
}

type RegisterReply struct {
	ID int
}

type PrepareArgs struct {
	TxID string
	Data string
}
