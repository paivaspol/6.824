package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "os"
import "syscall"
import "math/rand"


type PBServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool           // for testing
  unreliable bool     // for testing
  me string
  vs *viewservice.Clerk        // A PBServer instance is a client of the Viewservice.
  // Your declarations here.
  viewnum uint        // Viewnum to ping to viewservice. Follow received viewnums if 
  // PBServer instance is Primary/Backup. Otherwise, ping 0 to indicate idle.
  role string         // Role of the PBServer instance. Ex. "primary", "backup", "idle"
  kvstore map[string]string   // Map for Key/Value data of the key/value pbservice are stored.
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()
  fmt.Println("get: %s -> ", args.Key)

  value, present := pb.kvstore[args.Key]
  if present {
    reply.Value = value
    reply.Err = OK
  } else {
    reply.Value = ""
    reply.Err = ErrNoKey
  }

  // done preparing the reply
  return nil
}


func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()
  fmt.Println("put: %s -> %s", args.Key, args.Value)

  pb.kvstore[args.Key] = args.Value

  reply.Err = OK

  // done preparing the reply
  return nil
}


//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
  view, error := pb.vs.Ping(pb.viewnum)
  if error != nil {
    fmt.Println("error while pinging")
  }
  fmt.Println(view)
  pb.role, pb.viewnum = read_view(pb.me, view)
  fmt.Println("New:", pb.role, pb.viewnum)
  
}


/*
Read a View returned by the viewservice and determine the role the named PBServer instance
should take and the viewnum it should send to the viewservice when pinging.
Accepts pbserver_name, the .me field of the PBServer, and the a viewserver.View instance.
Returns a role string and viewnum uint for the PBServer.
*/
func read_view(pbserver_name string, view viewservice.View) (role string, viewnum uint) {
  if pbserver_name == view.Primary {
    return "primary", view.Viewnum 
  } else if pbserver_name == view.Backup {
    return "backup", view.Viewnum
  }
  return "idle", 0
}








// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  // Your pb.* initializations here.
  pb.role = "idle"
  pb.viewnum = 0
  pb.kvstore = map[string]string{}   // initialize Key/Value Storage map

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
  }()

  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
  }()

  return pb
}
