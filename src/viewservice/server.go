package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string

  view View                           // current viewservice View            
  // viewservice clients are potential P/B servers
  client_map map[string]PingData     // key: client name - > value time.Time of most recent ping received.
}

type PingData struct {
  time time.Time
  viewnum uint
}



//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
  fmt.Println(args.Me)

  // Add or update viewservice client's entry in client_map of most recent ping times.
  vs.client_map[args.Me] = PingData{time: time.Now(), viewnum: args.Viewnum}
  //fmt.Println(vs.client_map)

  reply.View = vs.view
  // done preparing the reply
  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
  fmt.Println("Server Get called. Client wants to know the View state")
  // Your code here.
  //reply.View = View{Viewnum: 1, Primary: "cat", Backup: "dog"}

  reply.View = vs.view
  // done preparing the reply
  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  // Initially no Primary and no Backup
  if vs.view.Primary == "" {
    fmt.Println("There is no primary")
    for name, ping_data := range vs.client_map {
      // idle clients send pings with viewnum 0
      if ping_data.viewnum == 0 && name != vs.view.Backup {
        fmt.Println("Choosing primary")
        vs.view.Viewnum += 1
        vs.view.Primary = name
      }
    }

  } else if vs.view.Backup == "" {
    fmt.Println("There is no backup")
    for name, ping_data := range vs.client_map {
      // idle clients send pings with viewnum 0
      if ping_data.viewnum == 0 && name != vs.view.Primary {
        fmt.Println("Choosing backup")
        vs.view.Viewnum += 1
        vs.view.Backup = name
      }
    }

  }

  fmt.Println(vs.view)

  // Your code here.
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)     // Return ptr to ViewServer struct with zero-valued fields.
  vs.me = me
  // Your vs.* initializations here.
  vs.view = View{}         // initial current view has viewnum 0
  vs.client_map = map[string]PingData{} 

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
