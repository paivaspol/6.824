package lockservice

import (
  "net/rpc"
  "fmt"
)

/*
  lockservice Clerk lives in the client and wraps the server's exposed LockServer methods.
  Clerk provides client programs with stubs for LockServer's Lock and Unlock methods; calling the stubs
  generated RPC calls which request the appropriate service and return the response.
*/
type Clerk struct {
  servers [2]string                 // primary port, backup port
  id int                            // unqiue clerk_id serves as a client identifier
  request_id_generator func() int   // func to generate next unqiue(among other requests by this Clerk) request id.
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

// Unique id generator for initializing Clerks
var clerk_id_generator = make_id_generator()


func MakeClerk(primary string, backup string) *Clerk {
  ck := new(Clerk)
  ck.servers[0] = primary
  ck.servers[1] = backup
  ck.id = clerk_id_generator()              
  ck.request_id_generator = make_id_generator()
  return ck
}

//
// ask the lock service for a lock.
// returns true if the lock service
// granted the lock, false otherwise.
//
// you will have to modify this function.
//
func (ck *Clerk) Lock(lockname string) bool {
  // prepare the arguments.
  args := &LockArgs{}                           // Declare and init struct with zero-valued fields. Return ptr to the struct.
  args.Lockname = lockname
  args.Client_id = ck.id                        // Clerk id is a unique client identifier
  args.Request_id = ck.request_id_generator()   // Each rpc request will have a unique identifier
  var reply LockReply                           // Declare
  
  // send an RPC request, wait for the reply.
  ok := call(ck.servers[0], "LockServer.Lock", args, &reply)

  if ok == false {
    // Call was not able to contact primary server.
    retry := call(ck.servers[1], "LockServer.Lock", args, &reply)
    
    if retry == false {
      // Cannot contact either server. This case is outside the scope of lab 1.
      return false
    }
    // Return the backup server's response to the client program.
    return reply.OK
  }

  // Return the primary server's response to the client program.
  return reply.OK
}


//
// ask the lock service to unlock a lock.
// returns true if the lock was previously held,
// false otherwise.
//
func (ck *Clerk) Unlock(lockname string) bool {
  // prepare the arguments
  args := &UnlockArgs{}                         // Declare and init struct with zero-valued fields. Return ptr to the struct.
  args.Lockname = lockname
  args.Client_id = ck.id                        // Clerk id is a unique client identifier
  args.Request_id = ck.request_id_generator()   // Each request will have a unique identifier
  var reply UnlockReply                         // Declare 

  // send an RPC request, wait for the reply.
  ok := call(ck.servers[0], "LockServer.Unlock", args, &reply)

  if ok == false {
    // Call was not able to contact primary server.
    retry := call(ck.servers[1], "LockServer.Unlock", args, &reply)

    if retry == false {
      // Cannot contact either server. This case is outside the scope of lab 1.
      return false
    }
    // Return the backup server's response to the client program
    return reply.OK
  }

  // Return the primary server's response to the client program.
  return reply.OK
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be the address
// of a reply structure.
//
// call() returns true if the server responded, and false
// if call() was not able to contact the server. in particular,
// reply's contents are valid if and only if call() returned true.
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


