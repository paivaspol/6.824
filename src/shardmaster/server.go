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

type Op struct {
  name string     // Operation name: Join, Leave, Move, Query
  args *Args
  // Your data here.
}

func makeOp(name string, args Args) (Op) {
  return Op{name: name,
            args: &args,
            }
}


func (self *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  // Your code here.
  output_debug(fmt.Sprintf("(server%d) Join \n", self.me))
  fmt.Println(args.GID)
  fmt.Println(args.Servers)
  fmt.Println(self.configs)

  operation := makeOp("join", *args)
  fmt.Println(operation)

  agreement_number := self.paxos_agree(operation)
  fmt.Println(agreement_number)

  self.perform_operations(agreement_number)

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

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  // Your code here.
  output_debug(fmt.Sprintf("(server%d) Query \n", sm.me))

  return nil
}

/*
Return the last operation_number that was performed on the local configuration 
state
*/
func (self *ShardMaster) last_operation_number() int {
  return self.operation_number
}

/*
Accepts an operation struct and drives agreement among Shardmaster paxos peers.
Returns the int agreement number the paxos peers collectively decided to assign 
the operation. Will not return until agreement is reached.
*/
func (self *ShardMaster) paxos_agree(operation Op) (int) {
  var agreement_number int
  var decided_operation = Op{}

  for decided_operation != operation {
    agreement_number = self.available_agreement_number()
    self.px.Start(agreement_number, operation)
    decided_operation = self.await_paxos_decision(agreement_number).(Op)  // type assertion
  }
  return agreement_number
}

/*
Returns the decision reached by the paxos peers for the given agreement_number. 
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

func (self *ShardMaster) perform_operations(limit int) {
  op_number := self.last_operation_number() + 1    // op number currently being performed
  has_decided, operation := paxos_status_op(self.px, op_number)

  for has_decided && op_number <= limit {
    output_debug(fmt.Sprintf("(server%d) Performing: op_num:%d lim:%d op:%v\n", self.me, op_number, limit, operation))
    self.perform_operation(operation)
    output_debug(fmt.Sprintf("(server%d) Applied: op_num:%d \n", self.me, op_number))
    op_number = self.last_operation_number() + 1
    has_decided, operation = paxos_status_op(self.px, op_number)
  }
  // next operation not yet known to local paxos instance be decided or limit reached
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

/*
Accepts an Op operation which should be performed locally, reads the name of the
operation and calls the appropriate handler
*/
func (self *ShardMaster) perform_operation(operation Op) {
  switch operation.name {
    case "join":
      fmt.Println("Join!!!!")
    case "leave":
      fmt.Println("Leave!!!!")
    case "move":
      fmt.Println("Move!!!!!")
    case "query":
      fmt.Println("Query!!!")
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
