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
  vs *viewservice.Clerk   // A PBServer instance is a client of the Viewservice.
  // Your declarations here.
  viewnum uint            // Viewnum to ping to viewservice. Follow received viewnums if 
  // PBServer instance is Primary/Backup. Otherwise, ping 0 to indicate idle.
  role string             // Role of the PBServer instance. Ex. "primary", "backup", "idle"
  primary string
  backup string
  //backup_active bool      // Ignored if role is primary/idle. Backup server will return ErrWrongServer errors
                          // for all forwarded  
  kvstore map[string]string   // Map for Key/Value data of the key/value pbservice are stored.
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  pb.mu.Lock()
  defer pb.mu.Unlock()

  if pb.role != "primary" {
    reply.Err = ErrWrongServer
    return nil
  }

  // Primary Server
  fmt.Printf("get: %s -> \n", args.Key)
  if pb.is_backup() {
    fmt.Println("There is a backup in place for get")
    fmt.Println(pb.backup)
    //ok := call()

  } 
  // Backup has been updated or there is no backup. Make local kvstore update and responding to client.
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

  if pb.role != "primary" {
    reply.Err = ErrWrongServer
    return nil
  }

  // Primary Server
  fmt.Printf("put: %s -> %s\n", args.Key, args.Value)

  if pb.is_backup() {
    fmt.Println("There is a backup in place for put")

  }
    // Backup has been updated or there is no backup. Make local kvstore update and responding to client.
  pb.kvstore[args.Key] = args.Value
  
  reply.Err = OK

  // done preparing the reply
  return nil
}


/*
Ping viewserver periodically to get latest view. Update the PBServer instance's primary
and backup fields and set the viewnum that this server should ping. Check the view against
the PBServer name to determine the 'role' this PBServer should act as.
Also, if Backup server changes, initiate a safe transfer of Key/Value data from the 
primary to the backup.
*/
func (pb *PBServer) tick() {
  pb.mu.Lock()
  defer pb.mu.Unlock()
  
  var original_backup = pb.backup
  fmt.Println("Original backup:", original_backup, pb.me)
  view, error := pb.vs.Ping(pb.viewnum)

  if error != nil {
    fmt.Println(error)

  } else {
    pb.role, pb.viewnum, pb.primary, pb.backup = read_view(pb.me, view)
    fmt.Printf("tick: %s, %d, P%s, B%s\n", pb.role, pb.viewnum, pb.primary, pb.backup)
    if pb.role == "primary" && original_backup != pb.backup && pb.backup != "" {
      // Initiate safe (locking) transfer of key/value pairs from primary to new backup.
      pb.transfer_kvstore()
    }
  } 

}

/*
A Primary Key/Value server, upon learning that the view has been changed to include a 
backup (which may still think it is idle) should lock the PBServer instance to prevent 
Gets/Puts and then transfer a copy of its key/value pairs to the backup.
Continue to attempt the transfer until success.
*/
func (pb *PBServer) transfer_kvstore() {
  // Does not need lock. Only called from tick, which is enclosed ina PBServer lock.
  
  fmt.Println("Transferring kvstore!", pb.me, pb.kvstore)

  var args = &KVStoreArgs{}   // declare and init struct with zero-valued fields. Reference struct.
  args.Kvstore = pb.kvstore
  var reply KVStoreReply      // declare reply to be populated by RPC
  // Attempt RPC call to transfer key/value pairs, until receiving successful response.
  for call(pb.backup, "PBServer.Receive_kvstore", args, &reply) == false {
    fmt.Println("Retry transfer of kvstore")
  }
  fmt.Println(reply)

}

/*
A Backup (or idle server which is about to become a Backup server, but the Primary received
the updated View first and initiated transfer) recevies Key/Value map data to initialize
its 
Do not worry about receiving requests, because the Primary does not allow requests until
the transfer is complete and the server is ready to serve as a backup.
*/
func (pb *PBServer) Receive_kvstore(args *KVStoreArgs, reply *KVStoreReply) error {
  pb.mu.Lock()                // Although a Primary should not be able to forward requests mid-transfer, lock the backup PBServer to be cautious.          
  defer pb.mu.Unlock()

  if pb.role == "backup" {
    fmt.Println("Receiving kvstore!", pb.me, pb.kvstore)
    fmt.Println(args.Kvstore)

    pb.kvstore = args.Kvstore
    reply.Err = OK
  } else {
    reply.Err = ErrWrongServer  // Server is currently idle and will become backup at next viewservice check
  }
  // done preparing the reply.
  return nil
}


/*
Read a View returned by the viewservice and determine the role the named PBServer instance
should take, the viewnum it should send to the viewservice when pinging, and the current
primary and backup server names.
Accepts pbserver_name, the .me field of the PBServer, and the a viewserver.View instance.
Returns a role string, viewnum uint for the PBServer, primary server name string, and 
backup server name string.
*/
func read_view(pbserver_name string, view viewservice.View) (role string, viewnum uint, primary string, backup string) {
  if pbserver_name == view.Primary {
    return "primary", view.Viewnum, view.Primary, view.Backup
  } else if pbserver_name == view.Backup {
    return "backup", view.Viewnum, view.Primary, view.Backup
  }
  return "idle", 0, view.Primary, view.Backup
}

/* Returns whether or not the current View has a backup. */
func (pb *PBServer) is_backup() bool {
  return !(pb.backup == "")
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
  pb.primary = ""
  pb.backup = ""
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
