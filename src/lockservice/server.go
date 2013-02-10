package lockservice

import "net"
import "net/rpc"
import "log"
import "sync"
import "fmt"
import "os"
import "io"
import "time"

type LockServer struct {
  mu sync.Mutex            // Will be used to synchronize access to the LockServer instance
  l net.Listener
  dead bool  // for test_test.go
  dying bool // for test_test.go

  am_primary bool // am I the primary?
  backup string   // backup's port

  locks map[string]bool               // key,value: lockname string, lock state bool
  old_requests map[int]map[int]bool   // key Client_id, Val map[int]bool: key Request_id, Value: bool OK value to be placed in response.

  // Go map note: reading non-present keys will return a zero valued bool, i.e. false.

}


/* server Lock RPC handler method.
   If the lockname was held, the client reply is an unsuccesful false. If the 
   lockname was not held, the lockname is held (or locked) and the client response
   is a successful true.
*/
func (ls *LockServer) Lock(args *LockArgs, reply *LockReply) error {
  ls.mu.Lock()
  defer ls.mu.Unlock()

  locked, _ := ls.locks[args.Lockname]

  if ls.am_primary {
    // Primary Server

    if locked {
      // lockname is currently locked, someone else may not lock it.
      reply.OK = false
    } else {
      // lockname is not locked. Lock it, notify backup server of the change to the locks map, and return true.
      ls.locks[args.Lockname] = true
      reply.OK = true
      fmt.Println("Notify backup server of Lock")
      call(ls.backup, "LockServer.Lock", args, &reply)
      // If rpc to backup fails, we technically do not need to bother sending it updates anymore since lab 1 guarantees at most 1 fail-stop.
    }
    // Done preparing the response.
    return nil

  } 

  // Backup Server
  fmt.Println(ls.old_requests)
  fmt.Println(args.Client_id)
  fmt.Println(args.Request_id)
  /* Backup must check whether it has seen the request before. Primary does not need to check because primary 
  never receives updates from another server, only directly from the client (and there are no network failures 
  so either primary gets the request and responds, or the primary crashes at some point and is no longer expected 
  to maintain synchronized data - backup is in charge now).*/
  ok_status, present := ls.old_requests[args.Client_id][args.Request_id]
  fmt.Println(ok_status, present)
  if present {
    fmt.Println("We've already seen this message!!!!")
    reply.OK = ok_status
    return nil
  }

  if locked {
    // lockname is currently locked, someone else may not lock it.
    reply.OK = false
    ls.old_requests[args.Client_id] = map[int]bool{args.Request_id: false}
  } else {
    // lockname is not locked. Lock it and return true.
    ls.locks[args.Lockname] = true
    reply.OK = true
    ls.old_requests[args.Client_id] = map[int]bool{args.Request_id: true}
  }
  // Done preparing the response.
  return nil 

}


/* server Unlock RPC handler method.
   If lockname was held, the lockname is released and the client reply is a successful
   true. If lockname was not held, the client reply is an unsuccessful false (and the
   lockname does not need to be released).
*/
func (ls *LockServer) Unlock(args *UnlockArgs, reply *UnlockReply) error {
  ls.mu.Lock()
  defer ls.mu.Unlock()

  locked, _ := ls.locks[args.Lockname]

  if ls.am_primary {
    // Primary Server

    if locked {
      // lockname is locked. Unlock it, notify backup server of the change, and return true.
      ls.locks[args.Lockname] = false
      reply.OK = true
      fmt.Println("Notify backup server of Unlock")
      call(ls.backup, "LockServer.Unlock", args, &reply)
      // If rpc to backup fails, backup must have failed and we technically do not need to update it any more.
    } else {
      // lockname is not locked. Return false reply.
      reply.OK = false
    }
    // Done preparing the response.
    return nil
  } 

  // Backup Server

  if locked {
    // lockname is locked. Unlock it, notify backup server of the change, and return true.
    ls.locks[args.Lockname] = false
    reply.OK = true
  } else {
    // lockname is not locked. Return false reply.
    reply.OK = false
  }
  // Done preparing the response.
  return nil 

}



//
// tell the server to shut itself down.
// for testing.
// please don't change this.
//
func (ls *LockServer) kill() {
  ls.dead = true
  ls.l.Close()
}

//
// hack to allow test_test.go to have primary process
// an RPC but not send a reply. can't use the shutdown()
// trick b/c that causes client to immediately get an
// error and send to backup before primary does.
// please don't change anything to do with DeafConn.
//
type DeafConn struct {
  c io.ReadWriteCloser
}
func (dc DeafConn) Write(p []byte) (n int, err error) {
  return len(p), nil
}
func (dc DeafConn) Close() error {
  return dc.c.Close()
}
func (dc DeafConn) Read(p []byte) (n int, err error) {
  return dc.c.Read(p)
}

func StartServer(primary string, backup string, am_primary bool) *LockServer {
  ls := new(LockServer)
  ls.backup = backup
  ls.am_primary = am_primary
  // Your initialization code here
  ls.locks = map[string]bool{}
  ls.old_requests = map[int]map[int]bool{}

  me := ""
  if am_primary {
    me = primary
  } else {
    me = backup
  }

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(ls)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(me) // only needed for "unix"
  l, e := net.Listen("unix", me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  ls.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for ls.dead == false {
      conn, err := ls.l.Accept()
      if err == nil && ls.dead == false {
        if ls.dying {
          // process the request but force discard of reply.

          // without this the connection is never closed,
          // b/c ServeConn() is waiting for more requests.
          // test_test.go depends on this two seconds.
          go func() {
            time.Sleep(2 * time.Second)
            conn.Close()
          }()
          ls.l.Close()

          // this object has the type ServeConn expects,
          // but discards writes (i.e. discards the RPC reply).
          deaf_conn := DeafConn{c : conn}

          rpcs.ServeConn(deaf_conn)

          ls.dead = true
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && ls.dead == false {
        fmt.Printf("LockServer(%v) accept: %v\n", me, err.Error())
        ls.kill()
      }
    }
  }()

  return ls
}
