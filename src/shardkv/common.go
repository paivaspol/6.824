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
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
)
type Err string

type Args interface{}
type OpResult interface{}
type Reply interface{}

type ReConfigStartArgs struct {}
type ReConfigStartReply struct {}
type ReConfigEndArgs struct {}
type ReConfigEndReply struct {}
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



