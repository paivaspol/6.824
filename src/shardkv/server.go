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

// field names in Paxos values should be capitalized. Paxos uses Go RPC library.
type Op struct {
  Id string      // uuid
  Name string    // Operation name: Get, Put, ConfigChange, Transfer, ConfigDone
  Args Args      // GetArgs, PutArgs, etc.    
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
  config_now shardmaster.Config     // latest Config of replica groups
  config_prior shardmaster.Config   // previous Config of replica groups
  operation_number int    // agreement number of latest applied operation
}


func (self *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  self.mu.Lock()
  defer self.mu.Unlock()
  debug(fmt.Sprintf("(svr:%d,rg:%d) Get: Key:%s (req: %d:%d)\n", self.me, self.gid, args.Key, args.Client_id, args.Request_id))


  return nil
}

func (self *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  self.mu.Lock()
  defer self.mu.Unlock()
  debug(fmt.Sprintf("(svr:%drg:%d) Put: Key:%s Val:%s (req: %d:%d)\n", self.me, self.gid, args.Key, args.Value, args.Client_id, args.Request_id))


  return nil
}

/*
Queries the ShardMaster for a new configuraton. If there is one, re-configure this
shardkv server to conform to the new configuration.
ShardKV server is a client of the ShardMaster service and can use ShardMaster client
stubs. 
*/
func (self *ShardKV) tick() {
  debug(fmt.Sprintf("(svr:%d,rg:%d) Tick: %+v \n", self.me, self.gid, self.config_now))

  config := self.sm.Query(-1)              // type ShardMaster.Config
  if config.Num > self.config_now.Num {
    // ShardMaster reporting a new Config
    self.config_prior = self.config_now
    self.config_now = config
  }


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

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)
  // Your initialization code here.
  // Don't call Join().
  kv.config_prior = ShardMaster.Config{}       // initial prior Config
  kv.config_prior.Groups = map[int64]string{}  // initialize map
  kv.operation_number = -1                     // first agreement number will be 0

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
