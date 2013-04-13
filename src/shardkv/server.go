package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"

const (
  Get = "Get"
  Put = "Put"
  Noop = "Noop"
)

// field names in Paxos values should be capitalized. Paxos uses Go RPC library.
/*
A single client request may create multiple Ops, but Op.Request_id is used in 
order to only perform the operation once.
*/
type Op struct {
  Id string          // uuid, identifies the operation itself
  //Request_id string  // combined, stringified client_id:request_id, identifies the client requested operation
  Name string        // Operation name: Get, Put, ConfigChange, Transfer, ConfigDone
  Args Args          // GetArgs, PutArgs, etc.    
}

func makeOp(name string, args Args) (Op) {
  return Op{Id: generate_uuid(),
            Name: name,
            Args: args,
            }
}


type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool               // for testing
  unreliable bool         // for testing
  sm *shardmaster.Clerk   // Shardkv is client of ShardMaster. Can use stubs.
  px *paxos.Paxos         // Shardkv is client of Paxos library.
  gid int64               // my replica group ID
  // Your definitions here.
  config_now shardmaster.Config    // latest Config of replica groups
  config_prior shardmaster.Config  // previous Config of replica groups
  operation_number int             // agreement number of latest applied operation
  storage map[string]string        // key/value data storage
  cache map[string]Reply           // "client_id:request_id" -> reply cache  
}

// Exported RPC functions (called by ShardKV clients)
///////////////////////////////////////////////////////////////////////////////

/*
Accepts a Get request, starts and awaits Paxos agreement for the op, performs all
operations up to and then including the requested operation.
Does not respond until the requested operation has been applied.
*/
func (self *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  self.mu.Lock()
  defer self.mu.Unlock()

  operation := makeOp(Get, *args)                      // requested Op
  agreement_number := self.paxos_agree(operation)      // sync call returns after agreement reached
  debug(fmt.Sprintf("(svr:%d,rg:%d) Get: Key:%s agree_num:%d (req: %d:%d)\n", self.me, self.gid, args.Key, agreement_number, args.Client_id, args.Request_id))

  self.perform_operations_prior_to(agreement_number)   // sync call, operations up to limit performed
  debug(fmt.Sprintf("(svr:%d,rg:%d) Get(ready2perform): Key:%s agree_num:%d (req: %d:%d)\n", self.me, self.gid, args.Key, agreement_number, args.Client_id, args.Request_id))
  op_result := self.perform_operation(agreement_number, operation)  // perform requested Op
  get_result := op_result.(GetReply)                   // type assertion
  reply.Value = get_result.Value
  reply.Err = get_result.Err
  return nil
}

/*
Accepts a Put request, starts and awaits Paxos agreement for the op, performs all
operations up to and then including the requested operation.
Does not respond until the requested operation has been applied.
*/
func (self *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  self.mu.Lock()
  defer self.mu.Unlock()

  operation := makeOp(Put, *args)                      // requested Op
  agreement_number := self.paxos_agree(operation)      // sync call returns after agreement reached
  debug(fmt.Sprintf("(svr:%d,rg:%d) Put: Key:%s Val:%s agree_num:%d (req: %d:%d)\n", self.me, self.gid, args.Key, args.Value, agreement_number, args.Client_id, args.Request_id))

  self.perform_operations_prior_to(agreement_number)   // sync call, operations up to limit performed
  debug(fmt.Sprintf("(svr:%d,rg:%d) Put(ready2perform): Key:%s Val:%s agree_num:%d (req: %d:%d)\n", self.me, self.gid, args.Key, args.Value, agreement_number, args.Client_id, args.Request_id))
  self.perform_operation(agreement_number, operation)  // perform requested Op

  reply.Err = OK
  return nil
}

/*
Queries the ShardMaster for a new configuraton. If there is one, re-configure this
shardkv server to conform to the new configuration.
ShardKV server is a client of the ShardMaster service and can use ShardMaster client
stubs. 
*/
func (self *ShardKV) tick() {
  // debug(fmt.Sprintf("(svr:%d,rg:%d) Tick: %+v \n", self.me, self.gid, self.config_now.Shards))
  config := self.sm.Query(-1)              // type ShardMaster.Config
  if config.Num > self.config_now.Num {
    // ShardMaster reporting a new Config
    self.config_prior = self.config_now
    self.config_now = config
  }

}


// Methods for Using the Paxos Library
///////////////////////////////////////////////////////////////////////////////

/*
Accepts an operation struct and drives agreement among Shardkv paxos peers.
Returns the int agreement number the paxos peers collectively decided to assign 
the operation. Will not return until agreement is reached.
*/
func (self *ShardKV) paxos_agree(operation Op) (int) {
  var agreement_number int
  var decided_operation = Op{}

  for decided_operation.Id != operation.Id {
    agreement_number = self.available_agreement_number()
    self.px.Start(agreement_number, operation)
    decided_operation = self.await_paxos_decision(agreement_number).(Op)  // type assertion
  }
  return agreement_number
}

/*
Returns the decision value reached by the paxos peers for the given agreement_number. 
This is done by calling the Status method of the local Paxos instance periodically,
frequently at first and less frequently later, using binary exponential backoff.
*/
func (self *ShardKV) await_paxos_decision(agreement_number int) (decided_val interface{}) {
  sleep_max := 10 * time.Second
  sleep_time := 10 * time.Millisecond
  for {
    has_decided, decided_val := self.px.Status(agreement_number)
    if has_decided {
      return decided_val
    }
    time.Sleep(sleep_time)
    if sleep_time < sleep_max {
      sleep_time *= 2
    }
  }
  panic("unreachable")
}

/*
Returns the next available agreement number (i.e. this paxos peer has not observed 
that a value was decided upon for the agreement number). This agreement number
may be tried when proposing new operations to peers.
*/
func (self *ShardKV) available_agreement_number() int {
  return self.px.Max() + 1
}

/*
Wrapper around the server's paxos instance px.Status call which converts the (bool,
interface{}) value returned by Paxos into a (bool, Op) pair. 
Accepts the agreement number which should be passed to the paxos Status call and 
panics if the paxos value is not an Op.
*/
func (self *ShardKV) px_status_op(agreement_number int) (bool, Op){
  has_decided, value := self.px.Status(agreement_number)
  if has_decided {
    operation, ok := value.(Op)    // type assertion, Op expected
    if ok {
        return true, operation
    }
    panic("expected Paxos agreement instance values of type Op at runtime. Type assertion failed.")
  }
  return false, Op{}
}

/*
Attempts to use the given operation struct to drive agreement among Shardmaster paxos 
peers using the given agreement number. Discovers the operation that was decided on
for the specified agreement number.
*/
func (self *ShardKV) drive_discovery(operation Op, agreement_number int) {
  self.px.Start(agreement_number, operation)
  self.await_paxos_decision(agreement_number)
}


// Methods for Performing ShardKV Operations
///////////////////////////////////////////////////////////////////////////////

/*
Synchronously performs all operations up to but NOT including the 'limit' op_number.
The set of operations to be performed may not all yet be known to the local paxos
instance so it will propose No_Ops to discover missing operations.
*/
func (self *ShardKV) perform_operations_prior_to(limit int) {
  op_number := self.operation_number + 1     // op number currently being performed
  has_decided, operation := self.px_status_op(op_number)

  for op_number < limit {       // continue looping until op_number == limit - 1 has been performed   
    //output_debug(fmt.Sprintf("(server%d) Performing_prior_to:%d op:%d op:%v %t\n", self.me, limit, op_number, operation, has_decided))
    if has_decided {
      self.perform_operation(op_number, operation)   // perform_operation mutates self.operation_number
      op_number = self.operation_number + 1
      has_decided, operation = self.px_status_op(op_number)
    } else {
      noop := makeOp(Noop, NoopArgs{})          // Force Paxos instance to discover next operation or agree on a Noop
      self.drive_discovery(noop, op_number)     // synchronously proposes Noop and discovered decided operation
      has_decided, operation = self.px_status_op(op_number)
      self.perform_operation(op_number, operation)
      op_number = self.operation_number + 1
      has_decided, operation = self.px_status_op(op_number)
    }
  }
}

/*
Accepts an Op operation which should be performed locally, reads the name of the
operation and calls the appropriate handler by passing the operation arguments.
Returns OpResult from performing the operation and increments (mutates) the ShardKV
operation_number field to the latest pperation (performed in increasing order).
*/
func (self *ShardKV) perform_operation(op_number int, operation Op) OpResult {
  debug(fmt.Sprintf("(srv:%d,rg:%d) Performing: op_num:%d op:%v", self.me, self.gid, op_number, operation))
  var result OpResult

  switch operation.Name {
    case "Get":
      var get_args = (operation.Args).(GetArgs)     // type assertion, Args is a GetArgs
      result = self.get(&get_args)
    case "Put":
      var put_args = (operation.Args).(PutArgs)   // type assertion, Args is a PutArgs
      result = self.put(&put_args)
    case "Noop":
      // zero-valued result of type interface{} is nil
    default:
      panic(fmt.Sprintf("unexpected Op name '%s' cannot be performed", operation.Name))
  }
  self.operation_number = op_number     // latest operation that has been applied
  self.px.Done(op_number)               // local Paxos no longer needs to remember Op
  debug(fmt.Sprintf("(srv%d,rg:%d) Performed: op_num:%d op:%v result:%+v", self.me, self.gid, op_number, operation, result))
  return result
}

// ShardKV RPC operations (internal, performed after paxos agreement)
///////////////////////////////////////////////////////////////////////////////

/*
Performs the operation if it has not already been applied 

Mutates Caller responsible for obtaining
a ShardMaster lock.
*/
func (self *ShardKV) get(args *GetArgs) OpResult {
  client_request := request_identifier(args.Client_id, args.Request_id) // string

  reply, present := self.cache[client_request]
  if present {
    fmt.Println("Already applied GET")
    return reply       // client requested get has already been performed
  }
  // client requested get has not been performed
  get_reply := GetReply{}
  value, present := self.storage[args.Key]
  if present {
    get_reply.Value = value
    get_reply.Err = OK
  } else {
    get_reply.Value = ""
    get_reply.Err = ErrNoKey
  }

  // cache put reply so duplicate client requests can be caught and not performed
  self.cache[client_request] = get_reply    
  return get_reply
}

/*

Mutates Caller responsible for obtaining
a ShardMaster lock.
Handles duplicate client requests (same client_id and request_id in the PutArgs) by
returning the cached reply for later duplicated attempts to perform the request.
*/
func (self *ShardKV) put(args *PutArgs) OpResult {
  client_request := request_identifier(args.Client_id, args.Request_id) // string

  reply, present := self.cache[client_request]
  // client requested put has already been performed
  if present {
    fmt.Println("Already applied PUT")
    return reply
  }
  // client requested put has not been performed

  self.storage[args.Key] = args.Value
  put_reply := PutReply{Err: OK}             // reply for successful Put request

  // cache put reply so duplicate client requests can be caught and not performed
  self.cache[client_request] = put_reply    
  return put_reply
}


// Helpers
///////////////////////////////////////////////////////////////////////////////

func request_identifier(client_id int, request_id int) string {
  return strconv.Itoa(client_id) + ":" + strconv.Itoa(request_id)
}










// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})
  gob.Register(GetArgs{})
  gob.Register(PutArgs{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)
  // Your initialization code here.
  // Don't call Join().
  kv.config_prior = shardmaster.Config{}  // initial prior Config
  kv.config_prior.Groups = map[int64][]string{}  // initialize map
  kv.operation_number = -1                // first agreement number will be 0
  kv.storage = map[string]string{}        // key/value data storage
  kv.cache =  map[string]Reply{}          // "client_id:request_id" -> reply cache  

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
