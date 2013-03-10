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
import "errors"

/* The 'values' Paxos will agree on are Op structs */
type Op struct {
  Kind string           // either GET_OP or PUT_OP
  Key string
  Value string          // will be "" for Get operations
}

type RequestLog struct {
  internal_log map[int]map[int]*Reply
}

func (self *RequestLog) entry_for(client_id int, request_id int) bool {
  _, present := self.internal_log[client_id][request_id]
  if present {
    return true
  }
  return false
}

func (self *RequestLog) logged_reply(client_id int, request_id int) (*Reply, error) {
  logged_reply, present := self.internal_log[client_id][request_id]
  if present {
    return logged_reply, nil
  }
  return nil, errors.New("no logged reply found")
}

// func (self *RequestLog) log(client_id int, request_id int, reply *Reply) error {
//   self.internal_log[client_id][request_id] = reply
//   if _, present := self.internal_log[client_id][request_id] == reply {
//     return nil
//   }
//   return errors.New("failed add record to RequestLog")
// }



type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  /* Paxos library instance which negotiates operation ordering and stores 
  log of recent operations until freed from memory.
  */
  px *paxos.Paxos
  // Key/Value Storage
  kvstore map[string]string    // Map for Key/Value data stored by the kvpaxos system
  // Prevent duplicate requests due to packet losses by storing replies
  //request_logs map[int]map[int]*interface{}
  request_log RequestLog
}



func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.


  return nil
}


func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  fmt.Printf("kvserver Put (server%d): Key: %s Value: %s\n", kv.me, args.Key, args.Value)
  client_id := args.get_client_id()
  request_id := args.get_request_id()
  key := args.get_key()
  value := args.get_value()

  // check request logs
  if kv.request_log.entry_for(client_id, request_id) {
    fmt.Println("found old log entry")
    reply, _ := kv.request_log.logged_reply(client_id, request_id)
    fmt.Println("Reply", reply)
  }

  fmt.Println(client_id)
  fmt.Println(request_id)
  fmt.Println(key)
  fmt.Println(value)
  operation := Op{Kind: "PUTOP", Key: key, Value: value}
  fmt.Println(operation)

  kv.px.Start(1, operation)

  to := 10 * time.Millisecond
  for {
    decided, _ := kv.px.Status(1)
    if decided {
      fmt.Println("Woo, decided")
      return nil 
    }
    time.Sleep(to)
    if to < 10 * time.Second {
      to *= 2
    }
  }



  //kv.log_request()

  return nil
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
  kv.kvstore = map[string]string{}        // initialize key/value storage map
  //kv.request_log = make(map[int]map[int]*interface{})

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

