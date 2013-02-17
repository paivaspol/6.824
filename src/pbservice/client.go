package pbservice

import "viewservice"
import "net/rpc"
//import "fmt"
// You'll probably need to uncomment this:
// import "time"



/*
The pbservice Clerk wraps the pbservice's servers' exposed PBServer methods.
The Clerk provides stubs for the PBServer's exposed methods so that calling a clerk stub 
generates an appropriate RPC call to the pbservice (more specifically, to the current
primary server in the pbservice).
Clerk maintains a bit of state about the name of the viewservice server it should talk to
to learn about the current View state.
*/
type Clerk struct {
  vs *viewservice.Clerk
}

func MakeClerk(vshost string, me string) *Clerk {
  ck := new(Clerk)
  ck.vs = viewservice.MakeClerk(me, vshost)
  return ck
}

//
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) (value string) {
  var args = &GetArgs{}   // declare and init struct with zero-valued fields. Reference struct.
  args.Key = key
  var reply GetReply      // declare reply to be poulated by RPC

  var primary_server = ck.vs.Primary()    // Clerk's viewservice Clerk's Primary stub retrieves primary name from viewservice.
  for call(primary_server, "PBServer.Get", args, &reply) == false {
    // repeat RPC call until Primary replies with success (i.e. OK)
  }
  if reply.Err == OK {
    return reply.Value
  }
  return ""               // Key does not exist   
}


//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
  var args = &PutArgs{}   // declare and init struct with zero-valued fields. Reference struct. 
  args.Key = key
  args.Value = value
  var reply PutReply      // declare reply to be populated by RPC

  var primary_server = ck.vs.Primary()    // Clerk's viewservice Clerk's Primary stub retrieves primary name from viewservice.  
  for call(primary_server, "PBServer.Put", args, &reply) == false {
    // repeat RPC call until Primary replies
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


