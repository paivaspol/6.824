package shardkv

import "shardmaster"
import "net/rpc"
import "time"
import "sync"
import "math/rand"
// import "fmt"

type Clerk struct {
  mu sync.Mutex               // one RPC at a time
  sm *shardmaster.Clerk       // App clients are clients of ShardMaster
  config shardmaster.Config   // client known latest Config of replica groups
  id int                      // unique id serves as a client identifier
  get_request_id func() int   // returns unique request ids (among requests by this client)
}


func MakeClerk(shardmasters []string) *Clerk {
  ck := new(Clerk)
  ck.sm = shardmaster.MakeClerk(shardmasters)
  ck.id = rand.Int()
  ck.get_request_id = make_int_generator()
  return ck
}

/*
Computes the shard the key belongs to and uses the current Config to determine the 
replica group and servers composing the replica group responsible for the shard. 
Attempts to fetch the current value for the key from each of the replica group 
servers until a reply is received which has Err set to OK or ErrNoKey. 
If trying all the servers in the replica group does not succeed, queries the
ShardMaster for the current configuration and tries again. Continues trying forever
until an appropriate reply is received.
*/
func (ck *Clerk) Get(key string) string {
  ck.mu.Lock()
  defer ck.mu.Unlock()
  client_id := ck.id
  request_id := ck.get_request_id()

  for {
    shard := key2shard(key)
    gid := ck.config.Shards[shard]
    servers, ok := ck.config.Groups[gid]

    if ok {
      // try each server in the shard's replication group.
      for _, srv := range servers {
        args := &GetArgs{}         // declare and init with zero-valued struct fields
        args.Client_id = client_id
        args.Request_id = request_id
        args.Key = key
        var reply GetReply         // declare reply, ready to be populated in RPC
        ok := call(srv, "ShardKV.Get", args, &reply)
        if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
          return reply.Value
        }
      }
    }
    time.Sleep(100 * time.Millisecond)

    // ask master for a new configuration.
    ck.config = ck.sm.Query(-1)
  }
  // should not reach this point
  return ""
}


/*
Computes the shard the key belongs to and uses the current Config to determine the 
replica group and servers composing the replica group responsible for the shard. 
Attempts to put the given key/value pair to each server in the replica group until
one of the servers replies indicating the operation was accepted and performed (i.e
reply.Err should be OK)
If trying all the servers in the replica group does not succeed, queries the
ShardMaster for the current configuration and tries again. Continues trying forever
until an appropriate reply is received.
*/
func (ck *Clerk) Put(key string, value string) {
  ck.mu.Lock()
  defer ck.mu.Unlock()
  client_id := ck.id
  request_id := ck.get_request_id()

  for {
    shard := key2shard(key)
    gid := ck.config.Shards[shard]
    servers, ok := ck.config.Groups[gid]

    if ok {
      // try each server in the shard's replication group.
      for _, srv := range servers {
        args := &PutArgs{}         // declare and init with zero-valued struct fields
        args.Client_id = client_id
        args.Request_id = request_id
        args.Key = key
        args.Value = value
        var reply PutReply         // declare reply, ready to be populated in RPC
        ok := call(srv, "ShardKV.Put", args, &reply)
        if ok && reply.Err == OK {
          return
        }
      }
    }

    time.Sleep(100 * time.Millisecond)

    // ask master for a new configuration.
    ck.config = ck.sm.Query(-1)
  }
}


// Helper Functions
///////////////////////////////////////////////////////////////////////////////

/*
Returns a function which is a generator of a deterministic (read: not unique across 
instances) sequence of unique, incrementing ints and provides such an int each time
it is called.
*/
func make_int_generator() (func() int) {
  base_id := -1
  return func() int {
    base_id += 1
    return base_id
  }
}

/*
Which shard is a key in? please use this function, and please do not change it.
*/
func key2shard(key string) int {
  shard := 0
  if len(key) > 0 {
    shard = int(key[0])
  }
  shard %= shardmaster.NShards
  return shard
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()
    
  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }
  return false
}


