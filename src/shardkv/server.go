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
  ReConfigStart = "ReConfigStart"
  ReConfigEnd = "ReConfigEnd"
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
  // Configuration Management
  config_now shardmaster.Config    // latest Config of replica groups
  config_prior shardmaster.Config  // previous Config of replica groups
  shards []bool                    // whether or not ith shard is present
  transition_to int                // Num of new Config transitioning to, -1 if not transitioning
  // Key/Value State
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

  if self.config_now.Num == 0 {
    return nil
  }

  operation := makeOp(Get, *args)                      // requested Op
  agreement_number := self.paxos_agree(operation)      // sync call returns after agreement reached
  debug(fmt.Sprintf("(svr:%d,rg:%d) Get: Key:%s agree_num:%d (req: %d:%d)", self.me, self.gid, args.Key, agreement_number, args.Client_id, args.Request_id))

  self.perform_operations_prior_to(agreement_number)   // sync call, operations up to limit performed
  debug(fmt.Sprintf("(svr:%d,rg:%d) Get(ready2perform): Key:%s agree_num:%d (req: %d:%d)", self.me, self.gid, args.Key, agreement_number, args.Client_id, args.Request_id))
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

  if self.config_now.Num == 0 {
    return nil
  }

  operation := makeOp(Put, *args)                      // requested Op
  agreement_number := self.paxos_agree(operation)      // sync call returns after agreement reached
  debug(fmt.Sprintf("(svr:%d,rg:%d) Put: Key:%s Val:%s agree_num:%d (req: %d:%d)", self.me, self.gid, args.Key, args.Value, agreement_number, args.Client_id, args.Request_id))

  self.perform_operations_prior_to(agreement_number)   // sync call, operations up to limit performed
  debug(fmt.Sprintf("(svr:%d,rg:%d) Put(ready2perform): Key:%s Val:%s agree_num:%d (req: %d:%d)", self.me, self.gid, args.Key, args.Value, agreement_number, args.Client_id, args.Request_id))
  self.perform_operation(agreement_number, operation)  // perform requested Op

  reply.Err = OK
  return nil
}

/*
Accepts a ReceiveShard, starts and awaits Paxos agreement for the op, performs all
operations up to and then including the requested operation.
Does not respond until the requested operation has been applied.
*/
func (self *ShardKV) ReceiveShard(args *ReceiveShardArgs, reply *ReceiveShardReply) error {
  self.mu.Lock()
  defer self.mu.Unlock()
  debug(fmt.Sprintf("(svr:%d,rg:%d) ReceiveShard:", self.me, self.gid))

  return nil
}

/*
Queries the ShardMaster for a new configuraton. If there is one, re-configure this
shardkv server to conform to the new configuration.
ShardKV server is a client of the ShardMaster service and can use ShardMaster client
stubs. 
*/
func (self *ShardKV) tick() {
  self.mu.Lock()
  defer self.mu.Unlock()
  //debug(fmt.Sprintf("(svr:%d,rg:%d) Tick: %+v", self.me, self.gid, self.config_now))

  if self.transition_to == -1 {         // Not currently changing Configs
    // Special initial case
    if self.config_now.Num == 0 {
      config := self.sm.Query(1)
      if config.Num == 1 {
        self.config_prior = self.config_now
        self.config_now = config
        // No shard transfers needed. Automatically have shards of first valid Config.
        self.shards = shard_state(self.config_now.Shards, self.gid)
        debug(fmt.Sprintf("(svr:%d,rg:%d) InitialConfig: %+v, %+v", self.me, self.gid, self.config_now, self.shards))
        return
      }
      // No Join has been performed yet. ShardMaster still has initial Config
      return
    }

    // are there new Configs we this replica group should be conforming to?
    config := self.sm.Query(-1)     // type ShardMaster.Config
    if config.Num > self.config_now.Num {      // ShardMaster reporting a new Config
      operation := makeOp(ReConfigStart, ReConfigStartArgs{})  // requested Op
      agreement_number := self.paxos_agree(operation)      // sync call returns after agreement reached
      debug(fmt.Sprintf("(svr:%d,rg:%d) ReConfigStart: agree_num:%d", self.me, self.gid, agreement_number))

      self.perform_operations_prior_to(agreement_number)   // sync call, operations up to limit performed
      debug(fmt.Sprintf("(svr:%d,rg:%d) ReConfigStart(ready2perform): agree_num:%d", self.me, self.gid, agreement_number))
      self.perform_operation(agreement_number, operation)  // perform requested Op
    }
    // Otherwise, no new Config and no action needed

  } else {                           // Currently changing Configs
    debug(fmt.Sprintf("(svr:%d,rg:%d) ConfigTransition: %+v, %+v", self.me, self.gid, self.config_now, self.shards))
    self.broadcast_shards()

    if self.done_sending_shards() && self.done_receiving_shards() {
      operation := makeOp(ReConfigEnd, ReConfigEndArgs{})  // requested Op
      agreement_number := self.paxos_agree(operation)      // sync call returns after agreement reached
      debug(fmt.Sprintf("(svr:%d,rg:%d) ReConfigEnd: agree_num:%d", self.me, self.gid, agreement_number))

      self.perform_operations_prior_to(agreement_number)   // sync call, operations up to limit performed
      debug(fmt.Sprintf("(svr:%d,rg:%d) ReConfigEnd(ready2perform): agree_num:%d", self.me, self.gid, agreement_number))
      self.perform_operation(agreement_number, operation)  // perform requested Op
    }
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
      var put_args = (operation.Args).(PutArgs)     // type assertion, Args is a PutArgs
      result = self.put(&put_args)
    case "ReConfigStart":
      var re_config_start_args = (operation.Args).(ReConfigStartArgs)  // type assertion
      result = self.re_config_start(&re_config_start_args)
    case "ReConfigEnd":
      var re_config_end_args = (operation.Args).(ReConfigEndArgs)     // type assertion
      result = self.re_config_end(&re_config_end_args)
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

// Methods for Managing Shard State
///////////////////////////////////////////////////////////////////////////////

/*
Checks against self.config_now.Shards to see if the shardkv server is/(will be once
ongoing xfer complete) responsible for the given key. Returns true if so and false
otherwise.
*/
func (self *ShardKV) owns_shard(key string) bool {
  shard_index := key2shard(key)
  return self.config_now.Shards[shard_index] == self.gid
}


/*
Returns whether or not the shardkv server currently has the shard corresponding
to the given stirng key. Consults the self.shards []bool state slice which 
represents which shards are present. self.shards is kept up to date during shard 
transfers that occur during config transitions.
*/
func (self *ShardKV) has_shard(key string) bool {
  shard_index := key2shard(key)
  return self.shards[shard_index]
}

/*
Converts a shards array of int64 gids (such as Config.Shards) into a slice of
booleans of the same length where an entry is true if the gid of the given 
shards array equals my_gid and false otherwise.
*/
func shard_state(shards [shardmaster.NShards]int64, my_gid int64) []bool {
  shard_state := make([]bool, len(shards))
  for shard_index, gid := range shards {
    if gid == my_gid {
      shard_state[shard_index] = true
    } else {
      shard_state[shard_index] = false
    }
  }
  return shard_state
}

func (self *ShardKV) done_sending_shards() bool {
  goal_shards := shard_state(self.config_now.Shards, self.gid)
  for shard_index, _ := range self.shards {
    if self.shards[shard_index] == true && goal_shards[shard_index] == false {
      // still at least one send has not been acked
      return false
    }
  } 
  return true
}

func (self *ShardKV) done_receiving_shards() bool {
  goal_shards := shard_state(self.config_now.Shards, self.gid)
  for shard_index, _ := range self.shards {
    if self.shards[shard_index] == false && goal_shards[shard_index] == true {
      // still at least one send has not been received
      return false
    }
  } 
  return true
}

func (self *ShardKV) broadcast_shards() {
  goal_shards := shard_state(self.config_now.Shards, self.gid)
  for shard_index, _ := range self.shards {
    if self.shards[shard_index] == true && goal_shards[shard_index] == false {
      // shard_index should be transferred to gid in new config
      new_replica_group_gid := self.config_now.Shards[shard_index]
      self.send_shard(shard_index, new_replica_group_gid)
    }
  } 
  return
}

type KVPair struct {
  Key string
  Value string
}

func (self *ShardKV) send_shard(shard_index int, gid int64) {
  fmt.Printf("Transferring shard %d to %d\n", shard_index, gid)
  // collect the key/value pairs that are part of the shard to be transferred
  var kvpairs []KVPair
  for key,value := range self.storage {
    if key2shard(key) == shard_index {
      kvpairs = append(kvpairs, KVPair{Key: key, Value: value})
    }
  }
  fmt.Println(kvpairs)
  servers := self.config_now.Groups[gid]
  next_rg_server := servers[rand.Intn(len(servers))]
  fmt.Println(servers, next_rg_server)

  args := &ReceiveShardArgs{}    // declare and init struct with zero-valued fields
  //args.kvpairs = kvpairs
  reply := ReceiveShardReply{}  // 
  // Attempt to send shard to random server in replica group now owning the shard
  ok := call(next_rg_server, "ShardKV.ReceiveShard", args, &reply)
  if ok {
    fmt.Println("Woo!")
  } else {
    fmt.Println("Lame")
  }



}

// ShardKV RPC operations (internal, performed after paxos agreement)
///////////////////////////////////////////////////////////////////////////////

/*
If the key to get is in a shard that is owned by the shardkv's replica group and 
present then a standard get is performed and the reply cached and returned. If the 
the key's shard is owned by the replica group, but not yet present, a reply with
an error is returned, but the reply is not cached since the client is expected to
retry at a later point. Finally, if the key is in a shard that is not owned by the 
replica group, an ErrWrongGroup reply is cached and returned.
Caller responsible for attaining lock on shardkv properties.
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

  if self.owns_shard(args.Key) {
    // currently or soon to be responsible for the key
    if self.has_shard(args.Key) {
      // currently has the needed shard (note: config transition may still be in progress)
      value, present := self.storage[args.Key]
      if present {
        get_reply.Value = value
        get_reply.Err = OK
      } else {
        get_reply.Value = ""
        get_reply.Err = ErrNoKey
      }
      // cache get reply so duplicate client requests not performed
      self.cache[client_request] = get_reply    
      return get_reply
    }
    // waiting to receive the shard
    get_reply.Err = NotReady
    // do not cache, expecting client to retry after transition progress
    return get_reply
  }
  // otherwise, the replica group does not own the needed shard
  get_reply.Err = ErrWrongGroup
  self.cache[client_request] = get_reply
  return get_reply   
}

/*
*/
func (self *ShardKV) put(args *PutArgs) OpResult {
  client_request := request_identifier(args.Client_id, args.Request_id) // string

  reply, present := self.cache[client_request]
  if present {
    fmt.Println("Already applied PUT")
    return reply                   // client requested put has already been performed
  }

  // client requested put has not been performed
  put_reply := PutReply{}

  if self.owns_shard(args.Key) {
    // currently or soon to be responsible for the key
    if self.has_shard(args.Key) {
      // currently has the needed shard (note: config transition may still be in progress)
      self.storage[args.Key] = args.Value
      put_reply := PutReply{Err: OK}             // reply for successful Put request

      // cache put reply so duplicate client requests not performed
      self.cache[client_request] = put_reply    
      return put_reply
    }
    // waiting to receive shard
    put_reply.Err = NotReady
    // do not cache, expecting client to retry after transition progress
    return put_reply
  }

  // otherwise, the replica group does not own the needed shard
  put_reply.Err = ErrWrongGroup
  self.cache[client_request] = put_reply
  return put_reply
}

/*
Local or fellow ShardKV peer has detected that ShardMaster has a more recent Config.
All peers have paxos agreed that this operation marks the transition to the next 
higher Config. Fetch the numerically next Config from the ShardMaster and fire first
set of shard transfer broadcasts. Also set shard_transition_period to true. The
shard_transition_period will be over once a peers paxos agree on a ReConfigEnd 
operation.
*/
func (self *ShardKV) re_config_start(args *ReConfigStartArgs) OpResult {

  if self.transition_to == self.config_now.Num {  // If currently transitioning
    // Multiple peers committed ReConfigStart operations, only need to start once
    fmt.Printf("Already transitioning to %d\n", self.transition_to)
    return nil
  }
  next_config := self.sm.Query(self.config_now.Num + 1)    // next Config
  self.config_prior = self.config_now
  self.config_now = next_config
  self.transition_to = self.config_now.Num

  self.shards = shard_state(self.config_prior.Shards, self.gid)
  goal_shards := shard_state(self.config_now.Shards, self.gid)
  debug(fmt.Sprintf("(svr:%d,rg:%d) reconfigstart: trans_to: %d, %+v, %v, %v\n", self.me, self.gid, self.transition_to, self.config_now, self.shards, goal_shards))

  return nil
}

func (self *ShardKV) receive_shard(args *ReceiveShardArgs) OpResult {

  return nil
}

/*
Local or fellow ShardKV peer has successfully recieved all needed shards for the
new Config (thus other peers can determine from the paxos log) and has received
acks from replica groups that were to receive shards from this replica group during
the transition tofrom all replica groups that 
*/
func (self *ShardKV) re_config_end(args *ReConfigEndArgs) OpResult {
  if self.transition_to == -1 {
    fmt.Println("Already stopped transitioning")
    return nil
  }
  self.transition_to = -1            // no longer in transition to a new config
  return nil
}


// Helpers
///////////////////////////////////////////////////////////////////////////////

func request_identifier(client_id int, request_id int) string {
  return strconv.Itoa(client_id) + ":" + strconv.Itoa(request_id)
}

func internal_request_identifier(client_id int, request_id int) string {
  return "i" + request_identifier(client_id, request_id)
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
  gob.Register(ReConfigStartArgs{})
  gob.Register(ReConfigEndArgs{})
  gob.Register(ReceiveShardArgs{})
  gob.Register(NoopArgs{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)
  // Your initialization code here.
  // Don't call Join().
  kv.config_prior = shardmaster.Config{}  // initial prior Config
  kv.config_prior.Groups = map[int64][]string{}  // initialize map
  kv.config_now = shardmaster.Config{}  // initial prior Config
  kv.config_now.Groups = map[int64][]string{}  // initialize map
  kv.shards = make([]bool, shardmaster.NShards)
  kv.transition_to = -1
  kv.storage = map[string]string{}        // key/value data storage
  kv.cache =  map[string]Reply{}          // "client_id:request_id" -> reply cache
  kv.operation_number = -1                // first agreement number will be 0


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
