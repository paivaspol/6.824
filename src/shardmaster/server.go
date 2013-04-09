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
  "strconv"
)

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

const (
  Join = "Join"
  Leave = "Leave"
  Move = "Move"
  Query = "Query"
  Noop = "Noop"
)

func strange(business interface{}) interface{} {
  return business
}

type Op struct {
  id string       // uuid
  name string     // Operation name: Join, Leave, Move, Query, Noop
  args Args
}

func makeOp(name string, args Args) (Op) {
  return Op{id: generate_uuid(),
            name: name,
            args: args,
            }
}


/*
TODO: Does not actually return a UUID according to standards. Just a random
in which suffices for now.
*/
func generate_uuid() string {
  return strconv.Itoa(rand.Int())
}

/*
Return the last operation_number that was performed on the local configuration 
state
*/
func (self *ShardMaster) last_operation_number() int {
  return self.operation_number
}

/*
Accepts a Join request, starts and awaits Paxos agreement for the op, and performs
all operations up to and including the requested operation.
Does not return until requested operation has been applied.
*/
func (self *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  var operation Op
  var agreement_number int

  output_debug(fmt.Sprintf("(server%d) Join %d %v %v\n", self.me, args.GID, args.Servers, self.configs))
  operation = makeOp(Join, *args)

  // synchronous call returns once paxos agreement reached
  agreement_number = self.paxos_agree(operation)
  output_debug(fmt.Sprintf("(server%d) Join agreement \n", self.me))
  // synchronous call, waits for operations up to limit to be performed
  self.perform_operations_prior_to(agreement_number)
  self.perform_operation(agreement_number, operation)    // perform requested op

  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  // Your code here.
  output_debug(fmt.Sprintf("(server%d) Leave \n", sm.me))


  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  // Your code here.
  output_debug(fmt.Sprintf("(server%d) Move \n", sm.me))


  return nil
}

func (self *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  var operation Op
  var agreement_number int

  output_debug(fmt.Sprintf("(server%d) Query %d\n", self.me, args.Num))
  operation = makeOp(Query, *args)

  // synchronous call returns once paxos agreement reached
  agreement_number = self.paxos_agree(operation)
  output_debug(fmt.Sprintf("(server%d) Query agreement", self.me))
  // synchronous call, waits for operations up to limit to be performed
  self.perform_operations_prior_to(agreement_number)
  result := self.perform_operation(agreement_number, operation)    // perform requested op

  fmt.Println(result)
  reply.Config = result.(Config)   // type assertion

  return nil
 
}


func (self *ShardMaster) join(args *JoinArgs) Result {
  
  fmt.Println("Performing Join", args.GID, args.Servers, self.configs)
  prior_config := self.configs[len(self.configs)-1]   // previous Config in ShardMaster.configs

  config := prior_config.copy()                       // newly created Config

  config.add_replica_group(args.GID, args.Servers)
  config.migrate_lonely_shards()
  config.rebalance(1)
  self.configs = append(self.configs, config)
  fmt.Println(self.configs)
  return nil
}

func (self *ShardMaster) leave(args *LeaveArgs) Result {

  fmt.Println("Performing Leave!!!!")

  return nil
}

func (self *ShardMaster) move(args *MoveArgs) Result {
  

  fmt.Println("Performing Move!!!!!")

  return nil
}

func (self *ShardMaster) query(args *QueryArgs) Result {
  

  fmt.Println("Performing Query!!!")
  // Fetch latest Config
  return self.configs[len(self.configs)-1]
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

  for decided_operation.id != operation.id {
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

  for has_decided && op_number < limit {
    output_debug(fmt.Sprintf("(server%d) Performing: op_num:%d lim:%d op:%v\n", self.me, op_number, limit, operation))
    self.perform_operation(op_number, operation)
    output_debug(fmt.Sprintf("(server%d) Applied: op_num:%d \n", self.me, op_number))
    op_number = self.last_operation_number() + 1
    has_decided, operation = self.px_status_op(op_number)
  }
  // next op not yet known to local paxos instance be decided or limit reached
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
  self.mu.Lock()
  defer self.mu.Unlock()
  output_debug(fmt.Sprintf("(server%d) Performing: op_num:%d op:%v\n", self.me, op_number, operation))
  var result Result
  switch operation.name {
    case "Join":
      var join_args = (operation.args).(JoinArgs)     // type assertion, Args is a JoinArgs
      result = self.join(&join_args)
    case "Leave":
      var leave_args = (operation.args).(LeaveArgs)   // type assertion, Args is a LeaveArgs
      result = self.leave(&leave_args)
    case "Move":
      var move_args = (operation.args).(MoveArgs)     // type assertion, Args is a MoveArgs
      result = self.move(&move_args)
    case "Query":
      var query_args = (operation.args).(QueryArgs)   // type assertion, Args is a QueryArgs
      result = self.query(&query_args)
    case "Noop":
      fmt.Println("Performing Noop!!!")
      result = 3
    default:
      panic("unexpected Op name cannot be performed")
      //result = 5
  }
  self.operation_number = op_number                   // latest operation that has been applied
  return result
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
