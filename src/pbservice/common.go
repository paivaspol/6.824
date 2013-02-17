package pbservice

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
)
type Err string

// Get RPC

type GetArgs struct {
  Key string
}

type GetReply struct {
  Err Err
  Value string
}

// Put RPC

type PutArgs struct {
  Key string
  Value string
}

type PutReply struct {
  Err Err
}

// Your RPC definitions here.

type KVStoreArgs struct {
	Kvstore map[string]string
}

type KVStoreReply struct {
	Err string
}

type InternalGetArgs struct {
	Key string
}

type InternalGetReply struct {
	Err Err
	Value string
}

type InternalPutArgs struct {
	Key string
	Value string
}

type InternalPutReply struct {
	Err Err
}


