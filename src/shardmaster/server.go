package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "reflect"

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

const = (
  Join = "Join"
  Leave = "Leave"
  Move = "Move"
  Query = "Query"
)

// type Op struct {
//   name string     // Operation name: Join, Leave, Move, Query, Noop
//   args Args
// }

// func makeOp(name string, args Args) (Op) {
//   return Op{name: name,
//             args: args,
//             }
// }

type Op struct {
  name string        // Operation name: Join, Leave, Move, Query, Noop
  gid int64          // Arg for Join, Leave, Move
  servers []string   // Arg for Join
  shard int          // Arg for Move
  num int            // Arg for Query
}

/*
Accepts a Join request, starts and awaits Paxos agreement for the op, and performs
all operations up to and including the requested operation.
Does not return until requested operation has been applied.
*/
func (self *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  output_debug(fmt.Sprintf("(server%d) Join \n", self.me))
  fmt.Println(args.GID, args.Servers, self.configs)
  operation := makeOp("join", *args)

  // synchronous call returns once paxos agreement reached
  agreement_number := self.paxos_agree(operation)
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
  output_debug(fmt.Sprintf("(server%d) Query \n", self.me))
  //fmt.Println(args.GID, args.Servers, self.configs)
  // operation := makeOp("query", *args)

  // // synchronous call returns once paxos agreement reached
  // agreement_number := self.paxos_agree(operation)
  // // synchronous call, waits for operations up to limit to be performed
  // self.perform_operations_prior_to(agreement_number)
  // self.perform_operation(agreement_number, operation)    // perform requested op

  return nil
}

/*
Return the last operation_number that was performed on the local configuration 
state
*/
func (self *ShardMaster) last_operation_number() int {
  return self.operation_number
}


func (self *ShardMaster) join(args *JoinArgs) {
  self.mu.Lock()
  defer self.mu.Unlock()

 
  fmt.Println("Got here")
  fmt.Println(args.GID)
  fmt.Println(args.Servers)

  fmt.Println(self.configs)
  fmt.Println(len(self.configs))

  current_config := len(self.configs)
  config := self.configs[current_config-1]
  fmt.Println(config)

  config2 := config.copy()
  config2.add_replica_group(args.GID, args.Servers)
  fmt.Println(config2)
  fmt.Println(config)

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
  var reflect.Value

  for decided_operation != operation {
    fmt.Println("Comparison")
    agreement_number = self.available_agreement_number()
    self.px.Start(agreement_number, operation)
    //decided_operation = self.await_paxos_decision(agreement_number).(Op)  // type assertion
    decided_operation := self.await_paxos_decision(agreement_number).(Op)

    fmt.Printf("%T, %v\n", operation.args, operation.args)
    fmt.Printf("%T, %v\n", decided_operation.args, decided_operation.args)

    val = reflect.ValueOf(decided_operation.args)
    decided_operation = val.(reflect.Typeof
    fmt.Printf("%T, %v\n", decided_operation.args, decided_operation.args)

    fmt.Println(operation == decided_operation)
    fmt.Println(operation)
    fmt.Println(decided_operation)
    fmt.Println("Success")
    fmt.Println(reflect.TypeOf(decided_operation))
  }
  fmt.Println("Never made it here")
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
Takes a paxos instance and an agreement number and calls px.Status to retrieve
the value that has been decided upon by the paxos peers.
Returns bool of whether decision reached and the value type asserted as an Op 
(empty Op if no decision was made yet).
Does NOT mutate the paxos instance.
*/
func paxos_status_op(px *paxos.Paxos, agreement_number int) (bool, Op){
  has_decided, value := px.Status(agreement_number)
  if has_decided {
    // type assertion. interface{} value should be an Op
    operation, ok := value.(Op)
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
  has_decided, operation := paxos_status_op(self.px, op_number)

  for has_decided && op_number < limit {
    output_debug(fmt.Sprintf("(server%d) Performing: op_num:%d lim:%d op:%v\n", self.me, op_number, limit, operation))
    self.perform_operation(op_number, operation)
    output_debug(fmt.Sprintf("(server%d) Applied: op_num:%d \n", self.me, op_number))
    op_number = self.last_operation_number() + 1
    has_decided, operation = paxos_status_op(self.px, op_number)
  }
  // next op not yet known to local paxos instance be decided or limit reached
}


/*
Accepts an Op operation which should be performed locally, reads the name of the
operation and calls the appropriate handler
*/
func (self *ShardMaster) perform_operation(op_number int, operation Op) {
  output_debug(fmt.Sprintf("(server%d) Performing: op_num:%d op:%v\n", self.me, op_number, operation))
  switch operation.name {
    case "join":
      fmt.Println("Performing Join!!!!")
      //var join_args = (*operation.args).(JoinArgs) // type assertion
      //fmt.Printf("%T, %v\n", join_args, join_args)
      //self.join(&join_args)
    case "leave":
      fmt.Println("Performing Leave!!!!")
    case "move":
      fmt.Println("Performing Move!!!!!")
    case "query":
      fmt.Println("Performing Query!!!")
    default:
      panic("unexpected Op name cannot be performed")
  }


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
