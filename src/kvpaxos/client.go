package kvpaxos

import "net/rpc"
import "time"
import "math/rand"

/*
client.go provides client applications with stubs to make Get and Put RPC 
calls to the kxpaxos distributed replicated key/value store system. 

Requests can be made to ANY of the kxpaxos servers running kvpaxos/server.go 
code which provides Get and Put handlers, replicated key/value data storage, 
operation ordering negotiation between Paxos peers (other kvpaxos servers)
through its paxos library instance (and thus logging of recent operations
which need to be applied to the replicated key/value store)

Strong consistency is maintained via the Paxos negotiated serial ordering of 
operations rather than directly exchanging key/value records between kvpaxos
server instances. No master server or viewservice, any server can respond to
any system request, and a minority of server failures are tolerated. 
Partitions that leave a majority of servers connected allow the system to 
continue operating on received requests
*/


type Clerk struct {
  servers []string
  // You'll have to add code here.
  id int                           // unique clerk_id serves as a client identifier.
  request_id_generator func() int  // returns unique request id (among requests by this Clerk).
}

/*
Maintains the collection of kvpaxos system servers that client RPC requests
should be directed to.
*/
func MakeClerk(servers []string) *Clerk {
  ck := new(Clerk)
  ck.servers = servers
  // You'll have to add code here.
  ck.id = rand.Int()
  ck.request_id_generator = make_id_generator()
  return ck
}


/*
  make_id_generator returns a function which will generate unique id's based on an enclosed base_id
  and incrementing the base_id by one each time the id_generator is called.
  The base_id starts at 0 so the first returned id is 1. This was done since it would be possible to 
  confuse an id of 0 with a default zero-valued int field (also 0).
*/
func make_id_generator() (func() int) {
  base_id := 0
  return func() int {
    base_id += 1
    return base_id
  }
}


/*
Client stub for generating a Get RPC request to fetch the current value for
a given string key. Retries with a different kvpaxos system server if there
is a failure.
Returns "" if the key does not exist and keeps trying forever in the face of 
all other errors.
*/
func (ck *Clerk) Get(key string) string {
  // You will have to modify this function.

  for {
    // try each known server.
    for _, srv := range ck.servers {
      args := &GetArgs{}            // declare and init with zero-valued struct fields, return ptr
      args.Client_id = ck.id        // unique client identifier
      args.Request_id = ck.request_id_generator()    // unique request id from this client.
      args.Key = key
      var reply GetReply            // declare reply, ready to be populated by RPC
      ok := call(srv, "KVPaxos.Get", args, &reply)
      if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
        return reply.Value
      }
    }
    time.Sleep(100 * time.Millisecond)
  }
  return ""
}


/*
Client stub for generating a Put RPC request to set the string value for a 
given string key. Retries with a different kvpaxos system server if there is
a failure.
Continues trying forever in the face of failures.
*/
func (ck *Clerk) Put(key string, value string) {
  // You will have to modify this function.

  for {
    // try each known server.
    for _, srv := range ck.servers {
      args := &PutArgs{}            // declare and init with zero-valued struct field
      args.Client_id = ck.id        // unique client identifier
      args.Request_id = ck.request_id_generator()  // unqiue request id from this client.
      args.Key = key
      args.Value = value
      var reply PutReply            // declare reply, ready to be populated by RPC
      ok := call(srv, "KVPaxos.Put", args, &reply)
      if ok && reply.Err == OK {
        return 
      }
    }
    time.Sleep(100 * time.Millisecond)
  }
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

