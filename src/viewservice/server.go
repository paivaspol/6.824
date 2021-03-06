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
  view View                          // current viewservice View            
  // viewservice clients are potential P/B servers
  client_map map[string]PingData     // key: client name - > value PingData struct of most recent ping data received.
}

type PingData struct {
  time time.Time
  latest_viewnum uint       // most recently pinged viewnum
  highest_viewnum uint      // highest viewnum the client has acked since last boot (dying and restarting should reset?)
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
  vs.mu.Lock()
  defer vs.mu.Unlock()
  //fmt.Printf("ping: from %s: latest_viewnum %d and highest_viewnum %d\n", "server" + args.Me[len(args.Me)-1:], args.Viewnum, vs.client_map[args.Me].highest_viewnum)
  // Add or update viewservice client's entry in client_map of client to client ping data.
  ping_data, found := vs.client_map[args.Me]
  if found {
    vs.client_map[args.Me] = PingData{
      time: time.Now(),
      latest_viewnum: args.Viewnum,
      highest_viewnum: maxUint(ping_data.highest_viewnum, args.Viewnum),
    }
  } else {
    vs.client_map[args.Me] = PingData{
      time: time.Now(), 
      latest_viewnum: args.Viewnum,
      highest_viewnum: args.Viewnum,
    }
  }
  // Always return current view, idle servers should keep sending pings with viewnum = 0
  reply.View = vs.view
  // done preparing the reply
  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
  // Always return current view, idle servers should keep sending pings with viewnum = 0
  reply.View = vs.view
  // done preparing the reply
  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly (if the Primary has acked).
//
func (vs *ViewServer) tick() {
  vs.mu.Lock()
  defer vs.mu.Unlock()

  //fmt.Print("vs_tick: ")
  //vs.dump_view_state()

  if vs.check_live(vs.view.Primary) && vs.check_live(vs.view.Backup) {
    // Both Primary and Backup are live (sent recent pings).

    // Check if Primary fail-restarted (i.e. failed and restarted without missing a Ping, so it now Pings viewnum = 0)
    if vs.is_idle(vs.view.Primary) && vs.primary_has_acked() {
      // Primary acked (so recognized it had become Primary) and then failed-restarted (now pings viewnum = 0)
      fmt.Println("Primary failed and restarted")
      // If Primary has not acked, cannot change viewservice view. 
      // Primary does not have data on it, promote backup 
      vs.attempt_promote_backup()
      // vs.view.Viewnum += 1
      // vs.view.Primary = vs.view.Backup        // backup may not be present
      // vs.view.Backup = ""
    }

    if vs.is_idle(vs.view.Backup) && vs.primary_has_acked() {
      fmt.Println("Backup fail restarted")
    }

  } else if vs.check_live(vs.view.Primary) && !vs.check_live(vs.view.Backup) {
    // Primary is currently alive and Backup is dead.

    // Check if Primary fail-restarted.
    if vs.is_idle(vs.view.Primary) && vs.primary_has_acked() {
      // Primary acked (so recognized it had become Primary) and then failed-restarted (now pings viewnum = 0)
      fmt.Println("Primary failed and restarted")
      // If Primary has not acked, cannot change viewservice view. 
      // Primary does not have data on it, promote backup 
      vs.attempt_promote_backup()
      // vs.view.Viewnum += 1
      // vs.view.Primary = vs.view.Backup        // backup may not be present
      // vs.view.Backup = ""
    }

    // Backup has not been initialized, it was promoted to Primary, or it died
    if vs.view.Backup == "" {
      // Backup client is set to "" (Backup has not been initialized or it was promoted). An idle client should replace it (increment View) or do nothing.
      //fmt.Println("Finding new backup")
      vs.attempt_assign_backup()
    } else {
      // Backup died and an idle client should replace it (increment view) or it should be set to "" (increment view)
      //fmt.Println("Need to replace dead backup")
      vs.attempt_remove_replace_backup()
    }

  } else if !vs.check_live(vs.view.Primary) && vs.check_live(vs.view.Backup) {
    // Primary is currently dead and Backup is alive

    if vs.is_idle(vs.view.Backup) && vs.primary_has_acked() {
      fmt.Println("Backup fail restarted")
    }

    // If Backup fail-restarted, it is as if a new Backup has been assigned. Replacing with a
    // new idle server (which also has no data) is pointless. Primary is responsible for replicating
    // records to the restarted Backup. 

    // Primary has died (no recent pings), attempt to promote backup
    vs.attempt_promote_backup()

  } else {
    // Either initialization or both Primary and Backup have died.

    if vs.view.Viewnum == 0 {
      // initialize viewservice with a Primary directly from idle clients
      name, found := vs.find_idle_client()
      if found {
        vs.view.Viewnum += 1
        vs.view.Primary = name
      }
    } else {
      // Perhaps the viewserivce is disconnected from Primary and Backup clients and they are still 
      // operating fine and network will be repaired. Wait. Otherwise intolerable P/B failure has 
      // occurred, primary and replicated data has all been lost, and system is not expected to recover.
    }

  }

}

/*
Helper method to search the viewservice client_map for an idle client. An idle
client is a live client which sends pings of viewnum 0 and is not the Primary or 
Backup in the viewservice's current view.
Returns the name of the found idle server and true if an idle server was found
and returns "" and false if no idle server was found.
*/
func (vs *ViewServer) find_idle_client() (name string, found bool) {
  for name, _ := range vs.client_map {
    if vs.is_idle(name) && name != vs.view.Primary && name != vs.view.Backup {
      return name, true
    }
  }
  return "", false
}

/*
Helper method checks whether the Primary client has ever acknowledged the current 
viewnum. Checks that the current viewnum is equal to the highest acked viewnum from
the Primary since the Primary can never ack a viewnum greater than the current 
viewnum (viewnum of the viewservice's current view never decreases).
*/
func (vs *ViewServer) primary_has_acked() bool {
  if vs.client_map[vs.view.Primary].highest_viewnum == vs.view.Viewnum {
    return true
  }
  return false
}

/*
Helper method to determine live/dead status. Consults the client_map and computes
the duration since the last Ping and compares with the DeadPing*PingInterval 
interval.
Accepts a client name 
Returns true if the client is alive and false otherwise (invalid client names cause
false to be returned)
*/
func (vs *ViewServer) check_live(name string) bool {
  dead_interval := DeadPings * PingInterval
  now := time.Now()
  last_ping, present := vs.client_map[name]
  if present && now.Sub(last_ping.time) < dead_interval {
    return true
  }
  return false
}

/*
Determines whether a live client's most recent Ping contained viewnum = 0, indicating it is 
an idle client (it may have crashed and restarted).
Accepts server name. Returns true if the client is live and its last ping contained viewnum = 0 
and returns false otherwise.
*/
func (vs *ViewServer) is_idle(name string) bool {
  last_ping, present := vs.client_map[name]
  if present && last_ping.latest_viewnum == 0 && vs.check_live(name) {
    return true
  }
  return false
}


/*
If the Primary has acked, an idle client is sought to replace the failed backup or
if none is found then the Backup is simply removed by setting it to "". In either
case, if Primary has acked, the View will be incremented and Backup changed.
If the Primary has not acked, the View may not be changed.
*/
func (vs *ViewServer) attempt_remove_replace_backup() {
  if vs.primary_has_acked() {
    name, found := vs.find_idle_client()
    if found {
      vs.view.Viewnum += 1
      vs.view.Backup = name 
    } else {
      vs.view.Viewnum += 1
      vs.view.Backup = ""
    }
  }

}


/*
If the primary has acked, an idle client (pinging viewnum = 0) is sought to be added 
as the new Backup (to initialize a backup or replace one that was promoted). If no idle 
client is found, the Backup is left as it is.
If the primary has not acked, the View may not be changed.
*/
func (vs *ViewServer) attempt_assign_backup() {
  if vs.primary_has_acked() {
    name, found := vs.find_idle_client()
    if found {
      vs.view.Viewnum += 1
      vs.view.Backup = name 
    } 
  }
}


/*
If the primary has acked (the current view is allowed to be updated) and backup
client is in place in the current view and alive (recent pings), promote it to
the Primary and leave the Backup empty (future ticks will attempt to assign a new
backup from the idle clients).
*/
func (vs *ViewServer) attempt_promote_backup() {
  if vs.primary_has_acked() && vs.check_live(vs.view.Backup) {
    vs.view.Viewnum += 1
    vs.view.Primary = vs.view.Backup
    vs.view.Backup = ""
  }
}

/*
If the primary has acked (the current view is allowed to be updated) and backup
client is in place in the current view and alive (recent pings), promote it to
the Primary and leave the Backup empty (future ticks will attempt to assign a new
backup from the idle clients).
*/
func (vs *ViewServer) attempt_promote_backup2() {
  if vs.primary_has_acked() && vs.check_live(vs.view.Backup) {
    vs.view.Viewnum += 1
    vs.view.Primary = vs.view.Backup
    vs.view.Backup = ""
  }
}

/*
Prints out the current viewservice ViewServer view and handles the case where Primary or Backup
are set to "".
*/
func (vs *ViewServer) dump_view_state() {
  if vs.view.Primary == "" && vs.view.Backup == "" {
    fmt.Printf("viewnum is %d, primary is %s, backup is %s\n", vs.view.Viewnum, vs.view.Primary, vs.view.Backup)
  } else if vs.view.Primary == "" {
    fmt.Printf("viewnum is %d, primary is %s, backup is %s\n", vs.view.Viewnum, vs.view.Primary, "server" + vs.view.Backup[len(vs.view.Backup)-1:])
  } else if vs.view.Backup == "" {
    fmt.Printf("viewnum is %d, primary is %s, backup is %s\n", vs.view.Viewnum, "server" + vs.view.Primary[len(vs.view.Primary)-1:], vs.view.Backup)
  } else {
    // Neither Primary nor Backup is set to ""
    fmt.Printf("viewnum is %d, primary is %s, backup is %s\n", vs.view.Viewnum, "server" + vs.view.Primary[len(vs.view.Primary)-1:], "server" + vs.view.Backup[len(vs.view.Backup)-1:])
  }
}


/*
Returns the maximum of two uints. If the two are equal, returns the second.
*/
func maxUint(x,y uint) uint {
  if x > y {
    return x
  }
  return y
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
