package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
  OK = "OK"
  NotReady = "NotReady"
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
)
type Err string

type Args interface{}
type OpResult interface{}
type Reply interface{}

type ReConfigStartArgs struct {}
type ReConfigEndArgs struct {}
type NoopArgs struct {}


// GetArgs and GetReply
///////////////////////////////////////////////////////////////////////////////

type GetArgs struct {
  Client_id int      // client_id
  Request_id int     // request_id unique per client
  Key string
}

type GetReply struct {
  Err Err
  Value string
}


// PutArgs and PutReply
///////////////////////////////////////////////////////////////////////////////

type PutArgs struct {
  Client_id int       // client_id
  Request_id int      // request_id unique per client
  Key string
  Value string
}

type PutReply struct {
  Err Err
}

// ReceiveShardArgs and ReceiveShardReply
///////////////////////////////////////////////////////////////////////////////

type ReceiveShardArgs struct {
  Kvpairs []KVPair     // slice of Key/Value pairs
  Trans_to int         // config number the sender is transitioning to
  Shard_index int      // index 
}

type ReceiveShardReply struct {
  Err Err
}



