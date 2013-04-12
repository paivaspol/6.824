package shardmaster

import (
  "net"
  "fmt"
  "net/rpc"
  "log"
  "paxos"
  "sync"
  "os"
  "syscall"
  "encoding/gob"
  "math/rand"
  //"reflect"
  "time"
)

const (
  Join = "Join"
  Leave = "Leave"
  Move = "Move"
  Query = "Query"
  Noop = "Noop"
)

// field names in Paxos values should be capitalized. Paxos library uses RPC library.
type Op struct {
  Id string       // uuid
  Name string     // Operation name: Join, Leave, Move, Query, Noop
  Args Args       // Args may be a JoinArgs, LeaveArgs, MoveArgs, or QueryArgs
}

func makeOp(name string, args Args) (Op) {
  return Op{Id: generate_uuid(),
            Name: name,
            Args: args,
            }
}

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool             // for testing
  unreliable bool       // for testing
  px *paxos.Paxos       // paxos library instance for comm with shardmaster peers
  configs []Config      // Go slice of config number indexed Configs
  operation_number int  // agreement number of latest applied operation
}

/*
Return the last operation_number that was performed on the local configuration 
state
*/
func (self *ShardMaster) last_operation_number() int {
  return self.operation_number
}

// Exported RPC functions (called by ShardMaster clients)
////////////////////////////////////////////////////////////////////////////

/*
Accepts a Join request, starts and awaits Paxos agreement for the op, and performs
all operations up to and including the requested operation.
Does not return until requested operation has been applied.
*/
func (self *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  self.mu.Lock()
  defer self.mu.Unlock()

  operation := makeOp(Join, *args)                    // requested Op
  agreement_number := self.paxos_agree(operation)     // sync call returns after agreement reached
  output_debug(fmt.Sprintf("(server%d) Join op_num:%d add:%d", self.me, agreement_number, args.GID))
  
  self.perform_operations_prior_to(agreement_number)  // sync call, operations up to limit performed
  output_debug(fmt.Sprintf("(server%d) Join(completedprior) op_num:%d add:%d", self.me, agreement_number, args.GID))
  self.perform_operation(agreement_number, operation) // perform requested Op
  
  // JoinReply does not have fields that must be populated
  return nil
}

func (self *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  self.mu.Lock()
  defer self.mu.Unlock()

  operation := makeOp(Leave, *args)                   // requested Op
  agreement_number := self.paxos_agree(operation)     // sync call returns after agreement reached
  output_debug(fmt.Sprintf("(server%d) Leave op_num:%d remove:%d", self.me, agreement_number, args.GID))

  self.perform_operations_prior_to(agreement_number)  // sync call, operations up to limit performed
  self.perform_operation(agreement_number, operation) // perform requested Op
  
  // LeaveReply does not have fields that must be populated
  return nil
}

func (self *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  self.mu.Lock()
  defer self.mu.Unlock()

  operation := makeOp(Move, *args)                    // requested Op
  agreement_number := self.paxos_agree(operation)     // sync call returns after agreement reached
  output_debug(fmt.Sprintf("(server%d) Move op_num:%d shard:%d moveto:%d", self.me, agreement_number, args.Shard, args.GID))

  self.perform_operations_prior_to(agreement_number)  // sync call, operations up to limit performed
  self.perform_operation(agreement_number, operation) // perform requested Op
  
  // MoveReply does not have fields that must be populated
  return nil
}

func (self *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  self.mu.Lock()
  defer self.mu.Unlock()

  operation := makeOp(Query, *args)                   // requested Op
  agreement_number := self.paxos_agree(operation)     // sync call returns after agreement reached
  output_debug(fmt.Sprintf("(server%d) Query op_num:%d config_num:%d", self.me, agreement_number, args.Num))

  self.perform_operations_prior_to(agreement_number)  // sync call, operations up to limit performed
  config := self.perform_operation(agreement_number, operation) // perform requested Op
  
  reply.Config = config.(Config)                      // type assertion
  return nil
}

// Methods for Using the Paxos Library
///////////////////////////////////////////////////////////////////////////////

/*
Accepts an operation struct and drives agreement among Shardmaster paxos peers.
Returns the int agreement number the paxos peers collectively decided to assign 
the operation. Will not return until agreement is reached.
*/
func (self *ShardMaster) paxos_agree(operation Op) (int) {
  var agreement_number int
  var decided_operation = Op{}

  for decided_operation.Id != operation.Id {
    agreement_number = self.available_agreement_number()
    //fmt.Printf("Proposing %+v with agreement_number:%d\n", operation, agreement_number)
    self.px.Start(agreement_number, operation)
    decided_operation = self.await_paxos_decision(agreement_number).(Op)  // type assertion
  }
  //output_debug(fmt.Sprintf("(server%d) Decided op_num:%d op:%v", self.me, agreement_number, decided_operation))
  return agreement_number
}

/*
Returns the decision value reached by the paxos peers for the given agreement_number. 
This is done by calling the Status method of the local Paxos instance periodically,
frequently at first and less frequently later, using binary exponential backoff.
*/
func (self *ShardMaster) await_paxos_decision(agreement_number int) (decided_val interface{}) {
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
func (self *ShardMaster) available_agreement_number() int {
  return self.px.Max() + 1
}

/*
Wrapper around the server's paxos instance px.Status call which converts the (bool,
interface{} value returned by Paxos into a (bool, Op) pair. 
Accepts the agreement number which should be passed to the paxos Status call and 
panics if the paxos value is not an Op.
*/
func (self *ShardMaster) px_status_op(agreement_number int) (bool, Op){
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
func (self *ShardMaster) drive_discovery(operation Op, agreement_number int) {
  self.px.Start(agreement_number, operation)
  self.await_paxos_decision(agreement_number)
}


// Methods for Performing ShardMaster Operations
///////////////////////////////////////////////////////////////////////////////

/*
Synchronously performs all operations up to but NOT including the 'limit' op_number.
The set of operations to be performed may not all yet be known to the local paxos
instance so it will propose No_Ops to discover missing operations.
*/
func (self *ShardMaster) perform_operations_prior_to(limit int) {
  op_number := self.last_operation_number() + 1    // op number currently being performed
  has_decided, operation := self.px_status_op(op_number)

  for op_number < limit {       // continue looping until op_number == limit - 1 has been performed   
    output_debug(fmt.Sprintf("(server%d) Performing_prior_to:%d op:%d op:%v %t\n", self.me, limit, op_number, operation, has_decided))
    if has_decided {
      self.perform_operation(op_number, operation)
      op_number = self.last_operation_number() + 1
      has_decided, operation = self.px_status_op(op_number)
    } else {
      noop := makeOp(Noop, NoopArgs{})             // Force Paxos instance to discover next operation or agree on a NO_OP
      self.drive_discovery(noop, op_number)  // proposes Noop or discovers decided operation.
      has_decided, operation = self.px_status_op(op_number)
      self.perform_operation(op_number, operation)
      op_number = self.last_operation_number() + 1
      has_decided, operation = self.px_status_op(op_number)
    }
  }
}

/*
Accepts an Op operation which should be performed locally, reads the name of the
operation and calls the appropriate handler by passing the operation arguments.
Obtains a Lock on the Server to perform the operation which mutates the Configs slice
so individual operations do not need to worry about obtaining locks. Returns the Result 
returned by the called operation and increments the ShardMaster operation_number to the
latest operation which has been performed (performed in increasing order).
*/
func (self *ShardMaster) perform_operation(op_number int, operation Op) Result {
  //self.mu.Lock()
  //defer self.mu.Unlock()
  output_debug(fmt.Sprintf("(server%d) Performing: op_num:%d op:%v", self.me, op_number, operation))
  var result Result

  switch operation.Name {
    case "Join":
      var join_args = (operation.Args).(JoinArgs)     // type assertion, Args is a JoinArgs
      result = self.join(&join_args)
    case "Leave":
      var leave_args = (operation.Args).(LeaveArgs)   // type assertion, Args is a LeaveArgs
      result = self.leave(&leave_args)
    case "Move":
      var move_args = (operation.Args).(MoveArgs)     // type assertion, Args is a MoveArgs
      result = self.move(&move_args)
    case "Query":
      var query_args = (operation.Args).(QueryArgs)   // type assertion, Args is a QueryArgs
      result = self.query(&query_args)
    case "Noop":
      // zero-valued result of type interface{} is nil
    default:
      panic(fmt.Sprintf("unexpected Op name '%s' cannot be performed", operation.Name))
  }
  self.operation_number = op_number     // latest operation that has been applied
  self.px.Done(op_number)               // local Paxos no longer needs to remember Op
  output_debug(fmt.Sprintf("(server%d) Performed: op_num:%d op:%v", self.me, op_number, operation))
  return result
}

// ShardMaster RPC operations (internal, performed after paxos agreement)
///////////////////////////////////////////////////////////////////////////////

/*
Creates a new Config by adding a new replica group, attempts to promote shards on invalid 
replica groups to valid replica groups (one may be available after adding a RG), and 
rebalances the shards.
Mutates ShardMaster.configs slice to append the new Config. Caller responsible for obtaining
a ShardMaster lock.
*/
func (self *ShardMaster) join(args *JoinArgs) Result {
  prior_config := self.configs[len(self.configs)-1]   // previous Config in ShardMaster.configs
  config := prior_config.copy()                       // newly created Config

  config.add_replica_group(args.GID, args.Servers)
  config.promote_shards_from_nonvalids()
  config.rebalance(1)
  self.configs = append(self.configs, config)
  return nil
}

/*
Creates a new Config by removing a replica group, reassigning shards that were assigned to
the RG to minimally loaded RGs, and rebalances the shards.
Mutates ShardMaster.configs slice to append the new Config. Caller responsible for obtaining
a ShardMaster lock.
*/
func (self *ShardMaster) leave(args *LeaveArgs) Result {
  prior_config := self.configs[len(self.configs)-1]   // previous Config in ShardMaster.configs
  config := prior_config.copy()                       // newly created Config
  
  config.remove_replica_group(args.GID)
  config.reassign_shards(args.GID)
  config.rebalance(1)
  self.configs = append(self.configs, config)
  return nil
}

/*
Creates a new Config by moving the specified shard to the speicifed replica group gid and
assumes that the gid is allowed (does not validate against Config.Groups).
Mutates ShardMaster.configs slice to append the new Config. Caller responsible for obtaining
a ShardMaster lock.
*/
func (self *ShardMaster) move(args *MoveArgs) Result {
  prior_config := self.configs[len(self.configs)-1]   // previous Config in ShardMaster.configs
  config := prior_config.copy()                       // newly created Config
  
  config.explicit_move(args.Shard, args.GID)
  // DO NOT rebalance. An explicit move was made by the administrator.
  // TODO: figure out exactly when migrate lonely shards should be done
  self.configs = append(self.configs, config)
  return nil
}

/*
Returns the Config numbered with the specified Num or returns the latest Config otherwise,
such as if Num = -1.
*/
func (self *ShardMaster) query(args *QueryArgs) Result {
  if args.Num >= 0 && args.Num <= (len(self.configs) - 1) {
    return self.configs[args.Num]
  } 
  // Otherwise, fetch the latest Config
  return self.configs[len(self.configs)-1]
}




// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})
  // RPC library needs to know how to marshall/unmarshall the different types of Args
  gob.Register(JoinArgs{})
  gob.Register(LeaveArgs{})
  gob.Register(MoveArgs{})
  gob.Register(QueryArgs{})
  gob.Register(NoopArgs{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)               // initially just a 0th Config
  sm.configs[0].Groups = map[int64][]string{}  // initialize map
  sm.operation_number = -1                     // first agreement number is 0

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
