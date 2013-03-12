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
  kvstore KVStorage           // Key/Value Storage
  reply_cache ReplyCache
}



func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  fmt.Printf("kvserver Get (server%d): Key: %s \n", kv.me, args.Key)

  client_id := args.get_client_id()
  request_id := args.get_request_id()
  key := args.get_key()
  fmt.Println(client_id)
  fmt.Println(request_id)
  fmt.Println(key)

  // check cached replies for request
  if kv.reply_cache.entry_exists(client_id, request_id) {
    reply, _ := kv.reply_cache.entry_lookup(client_id, request_id)
    fmt.Println("Reply", reply)
    // TODO construct reply to duplicate
  }

  

  operation := makeOp(client_id, request_id, "GET_OP", key, "")
  // negotiate the position of the operation in the ordering
  agreement_number, decided_operation := kv.agree_on_order(operation)

  fmt.Println(agreement_number, decided_operation)
  // TODO attempt to apply operations
  // TODO log the reply
  reply.Err = OK
  return nil
}


func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  fmt.Printf("kvserver Put (server%d): Key: %s Value: %s\n", kv.me, args.Key, args.Value)
  
  client_id := args.get_client_id()
  request_id := args.get_request_id()
  key := args.get_key()
  value := args.get_value()
  fmt.Println(client_id)
  fmt.Println(request_id)
  fmt.Println(key)
  fmt.Println(value)

  // check cached replies for request
  if kv.reply_cache.entry_exists(client_id, request_id) {
    reply, _ := kv.reply_cache.entry_lookup(client_id, request_id)
    fmt.Println("Reply", reply)
    // TODO construct reply to duplicate
  }

  operation := makeOp(client_id, request_id, "PUT_OP", key, value)
  // negotiate the position of the operation in the ordering
  agreement_number, decided_operation := kv.agree_on_order(operation)

  fmt.Println(agreement_number, decided_operation)
  // TODO attempt to apply operations
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
    decided, decided_value := kv.px.Status(agreement_number)
    if decided { 
      decided_operation, ok := decided_value.(Op)    // type assertion. interface{} value in Paxos instance is an Op
      if ok {
        return decided_operation
      }
      panic("expected Paxos agreement instance values of type Op at runtime. Type assertion failed.")
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

