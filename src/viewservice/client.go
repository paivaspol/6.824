package viewservice

import "net/rpc"
import "fmt"

/*
  A viewservice Client is an entity that pings the viewservice (to notify it that the client
  is alive and could serve as a Primary/Backup server) and makes viewservice Get requests to
  retrieve the current View state maintained by the viewservice.
*/


/*
  viewservice Clerk wraps the viewservice.server's exposed ViewServer methods.
  The Clerk provides stubs for the ViewServer's Ping and Get methods; calling a clerk stub 
  generates an RPC calls that make the appropriate requests of the viewservice
  and returns the response to the calling client (i.e. a Primary/Backup server)
  Also provides a convenient Primary stub which uses RPC communication with the viewservice's
  Get method to extract just the Primary client's name (viewservice is a centralized
  service to decide which of its client are the Primary and Backup.
  Clerk maintains a bit of state about the client name and the viewservice server name.
*/
type Clerk struct {
  me string      // client's name (host:port)
  server string  // viewservice's host:port
}


func MakeClerk(me string, server string) *Clerk {
  ck := new(Clerk)
  ck.me = me
  ck.server = server
  return ck
}

/*
  Sends a PingArgs struct indicating the viewservice client doing the pinging and the viewnum
  passed to the viewservice client Ping stub (i.e. the most recent view known by the viewservice
  client). Receives a PingReply in response containing the current View.
  If the RPC call succeeds, returns to the calling client the View from the viewservice reply
  and nil to indicate no erros occurred. Otherwise, returns a zero-valued View struct and a 
  string error message indicating that the ping failed (network problems or viewservice failure 
  - later not considered in this lab).
  */
func (ck *Clerk) Ping(viewnum uint) (View, error) {
  args := &PingArgs{}             // declare and init struct with zero-valued fields             
  args.Me = ck.me
  args.Viewnum = viewnum
  var reply PingReply

  // send an RPC request, wait for the reply.
  ok := call(ck.server, "ViewServer.Ping", args, &reply)
  if ok == false {
    return View{}, fmt.Errorf("Ping(%v) failed", viewnum)
  }

  return reply.View, nil
}


/*
  Requests the current View state from the viewservice. Sends an RPC call to the viewservice's
  ViewServer Get method. Expects server to modify and store a GetReply in the reply variable.
  If the RPC call succeeded, returns the View sent by the viewservice in the reply and true to
  indicate that the call succeeded. Otherwise returns a View with zero-valued fields and false.
  */
func (ck *Clerk) Get() (View, bool) {
  args := &GetArgs{}              // declare and init struct with zero-valued fields
  var reply GetReply              // declare reply so its ready to be modified by viewservice 
  ok := call(ck.server, "ViewServer.Get", args, &reply)
  if ok == false {
    return View{}, false
  }
  return reply.View, true
}

/*
  Helper stub which executes the Clerk Get stub and if the call succeeded, returns the primary
  server's name (port name). Otherwise returns an empty string.
  */
func (ck *Clerk) Primary() string {
  v, ok := ck.Get()
  if ok {
    return v.Primary
  }
  return ""
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

