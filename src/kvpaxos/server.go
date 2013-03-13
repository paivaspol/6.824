package kvpaxos

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
//import "errors"

/* The 'values' Paxos will agree on are Op structs */
type Op struct {
  Client_id int         // client id of the the requestor
  Request_id int        // request id
  Kind string           // "GET_OP", "PUT_OP", "NO_OP"
  Key string        
  Value string          // note: Get operations have Value ""
}

func makeOp(client_id int, request_id int, kind string, key string, value string) (Op) {
  return Op{Client_id: client_id, 
            Request_id: request_id, 
            Kind: kind, 
            Key: key, 
            Value: value}
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool          // for testing
  unreliable bool    // for testing
  // Paxos library instance; negotiates operation ordering, stores log of recent operations  
  px *paxos.Paxos
  kvcache KVCache      // Cache replies and applied operations
  kvstore KVStorage    // Key/Value Storage
}



func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()

  client_id := args.get_client_id()
  request_id := args.get_request_id()
  key := args.get_key()
  fmt.Printf("kvserver Get (server%d): Key: %s (req: %d:%d)\n", kv.me, key, client_id, request_id)

  // check cached replies for request
  if kv.kvcache.entry_exists(client_id, request_id) {
    reply, _ := kv.kvcache.cached_reply(client_id, request_id)
    fmt.Println("Cached entry exists", reply)
    // TODO construct reply to duplicate
  }

  operation := makeOp(client_id, request_id, "GET_OP", key, "")
  // negotiate the position of the operation in the ordering
  agreement_number, decided_operation := kv.agree_on_order(operation)

  fmt.Println(agreement_number, decided_operation)
  // TODO attempt to apply operations
  kv.apply_operations_to_kvstore()

  // await application of operation to kvstore
  value := kv.await_get_operation(agreement_number, decided_operation)

  // TODO log the reply
  reply.Err = OK
  reply.Value = value
  return nil
}


func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  
  client_id := args.get_client_id()
  request_id := args.get_request_id()
  key := args.get_key()
  value := args.get_value()
  fmt.Printf("kvserver Put (server%d): Key: %s Value: %s (req: %d:%d)\n", kv.me, key, value, client_id, request_id)

  // check cached replies for request
  if kv.kvcache.entry_exists(client_id, request_id) {
    reply, _ := kv.kvcache.cached_reply(client_id, request_id)
    fmt.Println("Cached entry exists", reply)
    // TODO construct reply to duplicate
  }

  operation := makeOp(client_id, request_id, "PUT_OP", key, value)
  // negotiate the position of the operation in the ordering
  agreement_number, decided_operation := kv.agree_on_order(operation)

  fmt.Println(agreement_number, decided_operation)
  // TODO attempt to apply operations
  kv.apply_operations_to_kvstore()
  // TODO log the reply
  reply.Err = OK
  return nil
}

/*
Drives Paxos agreement by proposing an Operation value for an agreement instance, 
awaiting the decision, checking the value of the decided Operation, and retries
proposing the Operation value with a new agreement instance until the Operation
value it proposes is the value that is decided upon for some agreement instance 
(giving the operation an agreed upon position in the operation ordering)
*/
func (kv *KVPaxos) agree_on_order(operation Op) (int, Op) {
  var agreement_number int
  var decided_operation = Op{}

  for decided_operation != operation {
    agreement_number = kv.next_agreement_number()
    decided_operation = kv.start_await_agreement(agreement_number, operation)
  }
  return agreement_number, decided_operation
}


/*
Starts a Paxos agreement instance and checks whether a decision has been made, 
making frequent checks at first and less frequent checks with binary backoff
later. Returns the Op that was decided upon by the Paxos peers.
*/
func (kv *KVPaxos) start_await_agreement(agreement_number int, operation Op) Op {
  kv.px.Start(agreement_number, operation)

  sleep_max := 10 * time.Second
  sleep_time := 10 * time.Millisecond
  for {
    has_decided, decided_op := kv.px_status_op_wrap(agreement_number)
    if has_decided {
      return decided_op
    }
    time.Sleep(sleep_time)
    if sleep_time < sleep_max {
      sleep_time *= 2
    }
  }
  panic("unreachable")
}


/*
Next operation should try to be assigned the next next available Paxos agreement 
instance number. Returns the number from the local Paxos peer for the Max agreement 
instance number it keeps in its logs plus 1. Note that this may not be the maximum
agreement instance number known across the system and the operation will have to
be re-proposed when the Paxos peer has learned more.
*/
func (kv *KVPaxos) next_agreement_number() int {
  return kv.px.Max() + 1
}


/*
Checks the Paxos instance log for the operation next expected by the kvstore (it
expects the operation numbered one higher than the its current 'operation_number' 
which reflects that it represents operations up to 'operation_number'.

Loops to continue requesting decided upon operations from the paxos instance
until reaching an agreement number which has not yet been decided. Applies all
operations it can to the kvstore, updates the kvcache to indicate operations
were applied. Get handler responsible for caching replies. Calls px.Done(x) after
applying the operation of each agreement instance to the kvstore since this 
kvserver will no longer need that entry in its paxos instance.

Assumes that agreements are made in increasing agreement number (starting at 0) 
order without skipping any numbers. The way kvserver calls kx.px.Start with an 
agreement number from kx.next_agreement_number() which returns one more than the
highest agreement number seen by the px paxos instance ensures this.
)
*/
func (kv *KVPaxos) apply_operations_to_kvstore() {
  op_number_to_apply := kv.kvstore.get_operation_number() + 1
  has_decided, operation := kv.px_status_op_wrap(op_number_to_apply)

  for has_decided {
    fmt.Printf("(server%d) apply: op_num: %d op: %v\n", kv.me, op_number_to_apply, operation)
    // apply operation and adjust kvstore's latest applied operation number
    kv.kvstore.apply_operation(operation, op_number_to_apply)

    op_number_to_apply = kv.kvstore.get_operation_number() + 1
    has_decided, operation = kv.px_status_op_wrap(op_number_to_apply)
  }
  fmt.Println("Finished applying operations to kvstore")
}


/*
Simply calls the paxos instance's Status method to determine whether a value has 
been decided for a particular agreement instance. However, since kvserver's only
start Paxos agreement with Op values, it is assumed that the agreed upon values 
will awlways be Op structs so this method does type assertion work to return an 
Op rather than a interface{}.
Returns boolean of whether agreement has been reached on the given agreement_number
and the agreed upon operation (or a zero-valued operation).
*/
func (kv *KVPaxos) px_status_op_wrap(agreement_number int) (bool, Op) {
  has_decided, decided_val := kv.px.Status(agreement_number)
  if has_decided {
    // type assertion. interface{} value should be an Op
    decided_op, ok := decided_val.(Op)
    if ok {
        return true, decided_op
    }
    panic("expected Paxos agreement instance values of type Op at runtime. Type assertion failed.")
  }
  return false, Op{}
}


func (kv *KVPaxos) await_get_operation(operation_number int, decided_value interface{}) (value string) {

  sleep_max := 10 * time.Second
  sleep_time := 10 * time.Millisecond
  for {
    if kv.kvstore.get_operation_number() >= operation_number {
      fmt.Println("Get has been applied already")

      decided_operation, _ := decided_value.(Op)    // type assertion. interface{} value in Paxos instance is an Op
      if decided_operation.Kind == "GET_OP" {
        fmt.Println("Perform GET_OP")
        value, _ := kv.kvstore.lookup(decided_operation.Key)
        fmt.Println(value)
        return value
      }
    }
    time.Sleep(sleep_time)
    if sleep_time < sleep_max {
      sleep_time *= 2
    }
  }
  panic("unreachable")

}






// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *KVPaxos {
  // this call is all that's needed to persuade
  // Go's RPC library to marshall/unmarshall
  // struct Op.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me
  // Your initialization code here.
  kv.kvstore = KVStorage{state: map[string]string{}, 
                         operation_number: -1,
                        }
  kv.kvcache = KVCache{state: make(map[int]map[int]*CacheEntry)}


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
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}

