package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

/*
Library implementing the Paxos protocol and managing a set of agreed upon values. Each
kvpaxos server will have an instance of this library which allows it to start agreement
negotiation about a new Paxos instance/run and retrieve the decision that was made. All 
library calls are stubs which use RPC communication with other peers to send messages 
back and forth and eventually reach agreements. To support systems that run for a long 
time, the agreement map is cleaned up by removing entries that all kvpaxos servers have 
indicated they will no longer need using the Done call.
*/

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"


type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int                         // index into peers[]
  peer_count int                 // Number of Paxos Peers
  state map[int]AgreementState   // Key: Agreement instance number -> Value: Agreement State
}


/*
The application wants paxos to start agreement instance with number sequence_number, 
with proposed value 'value'.
Start() returns right away; the application will call Status() to find out if/when 
agreement is reached.
*/
func (px *Paxos) Start(sequence_number int, proposal_value interface{}) {
  px.mu.Lock()
  defer px.mu.Unlock()
  fmt.Println(px.me, px.peers, px.state, sequence_number, proposal_value)

  agreement_state, present := px.state[sequence_number]
  if !present {
    fmt.Println("Initializing Agreement entry during Start")
    px.state[sequence_number] = AgreementState{}   // declare and init struct with zero-valued fields    
    agreement_state = px.state[sequence_number]
  } 

  // propose RPC call send to all peers
  var proposal_number = px.state[sequence_number].highest_seen + 1
  agreement_state.highest_seen += 1
  px.state[sequence_number] = agreement_state
  
  // Spawn a thread to act as the proposer
  go px.proposer_role(sequence_number, proposal_number, proposal_value)

  return
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  return 0
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// It is illegal to call Done(i) on a peer and
// then call Start(j) on that peer for any j <= i.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  // You code here.
  return 0
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peers state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(sequence_number int) (bool, interface{}) {
  // Your code here.
  agreement_state, present := px.state[sequence_number]
  if present && agreement_state.accepted_proposal != nil {
    return true, agreement_state.accepted_proposal.Value
  }
  return false, nil
}


/*
Acts in the Paxos proposer role to first propose a proposal to the acceptors for 
agreement instance 'sequence_number' and if a majority reply with prepare_ok then
the proposer proceeds to phase 2. In this phase, it sends an accept request for the
proposal for agreement instance 'sequence_number' to all the acceptors.
?????
*/
func (px *Paxos) proposer_role(sequence_number int, proposal_number int, proposal_value interface{}) {
  // Send RPC prepare request to each Paxos acceptor with a proposal for Agreement instance 'sequence_number'.

  var proposal_ptr = &Proposal{Number: proposal_number, Value: proposal_value}    // ptr to struct
  
  var replies_from_prepare = make([]PrepareReply, px.peer_count)                  // declare and init

  for index, peer := range px.peers {
    args := &PrepareArgs{}       // declare and init struct with zero-valued fields. 
    args.Sequence_number = sequence_number
    args.Proposal_number = proposal_number
    var reply PrepareReply       // declare reply so it is ready to be modified by called Prepare_handler
    // Attempt to contact peer. No reply is equivalent to a vote no.
    ok := call(peer, "Paxos.Prepare_handler", args, &reply)
    fmt.Println(ok)

    replies_from_prepare[index] = reply
  }
  fmt.Println("Prepare Replies", replies_from_prepare)

  var ok_count = 0       // Number of prepare replies that were prepare_ok
  var highest_proposal = &Proposal{}   // highest numbered proposal from replies
  for _, reply := range replies_from_prepare {
    if reply.Prepare_ok {
      // The acceptor replied with prepare_ok
      ok_count += 1
      if reply.Accepted_proposal != nil && reply.Accepted_proposal.Number > highest_proposal.Number {
        highest_proposal = reply.Accepted_proposal
      }
    }
  }
  fmt.Println(ok_count, px.peer_count / 2)
  if !px.is_majority(ok_count) {
    return
  }

  fmt.Println("Continue to accept requests")

  var replies_from_accept = make([]AcceptReply, px.peer_count)   // declare and init

  for index, peer := range px.peers {
    args := &AcceptArgs{}       // declare and init struct with zero-valued fields. 
    args.Sequence_number = sequence_number
    args.Proposal = proposal_ptr
    var reply AcceptReply       // declare reply so it is ready to be modified by called Accept_handler
    
    // Attempt to contact peer. No reply is equivalent to a vote no.
    call(peer, "Paxos.Accept_handler", args, &reply)
    
    replies_from_accept[index] = reply
  }
  fmt.Println("Accept Replies", replies_from_accept) 

  ok_count = 0
  for _, reply := range replies_from_accept {
    if reply.Accept_ok {
      ok_count += 1
    }
  } 

  fmt.Println(ok_count, px.peer_count / 2)
  if px.is_majority(ok_count) {
    fmt.Println("Majority accepted")
    // Decision has been made. Decision will never have a different value
    agreement_state := px.state[sequence_number]
    agreement_state.accepted_proposal = proposal_ptr
    px.state[sequence_number] = agreement_state
    fmt.Println("Decided on !! ", proposal_ptr)
  }

}




/*
Paxos library instances communicate with one another via exported RPC methods.
Accepts pointers to PrepageArgs and PrepareReply. Method will populate the PrepareReply
and return any errors.
*/
func (px *Paxos) Prepare_handler(args *PrepareArgs, reply *PrepareReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
  fmt.Println("prepare received ", px.peers[px.me])
  fmt.Println("Args", args)
  var sequence_number = args.Sequence_number
  var n = args.Proposal_number
  fmt.Println(px.state)
  agreement_state, present := px.state[sequence_number]
  if !present {
    fmt.Println("Initializing map entry")
    px.state[sequence_number] = AgreementState{}
    agreement_state = px.state[sequence_number]
  } 
  if n > agreement_state.highest_promised {
    agreement_state.highest_promised = n
    px.state[sequence_number] = agreement_state

    reply.Prepare_ok = true
    reply.Number_promised = n
    reply.Accepted_proposal = agreement_state.accepted_proposal
    return nil
  }
  reply.Prepare_ok = false
  return nil
}


/*
Paxos library instances communicate with one another via exported RPC methods.
Accepts pointers to PrepageArgs and PrepareReply. Method will populate the PrepareReply
and return any errors.
*/
func (px *Paxos) Accept_handler(args *AcceptArgs, reply *AcceptReply) error {
  fmt.Println("AcceptHandler")
  px.mu.Lock()
  defer px.mu.Unlock()

  var sequence_number = args.Sequence_number
  var proposal = args.Proposal
  fmt.Println(proposal)

  agreement_state, present := px.state[sequence_number]
  if !present {
    fmt.Println("Initializing map entry")
    px.state[sequence_number] = AgreementState{}
    agreement_state = px.state[sequence_number]
  } 

  if proposal.Number >= agreement_state.highest_promised {
    agreement_state.highest_promised = proposal.Number
    agreement_state.accepted_proposal = proposal
    px.state[sequence_number] = agreement_state

    reply.Accept_ok = true
    return nil
  }

  reply.Accept_ok = false
  return nil
}

/* 
Representation of a Proposal which is what a proposer proposes to acceptors which 
has a unique number per Agreement instance and a value that the proposer wants to 
become the decided value for the agreement instance
*/
type Proposal struct {
  Number int               // proposal number (unqiue per Argeement instance)
  Value interface{}        // value of the Proposal
}

/* 
Represents the state stored by a Paxos library instance per Agreement instance.
'number_promised' is the number returned in prepare_ok replies to promise not to 
accept any more proposals numbered less than 'number_promised'
accepted_proposal is the highest numbered proposal that has been accepted.
highest_seen is the highest proposal number the paxos library instance has seen - this
is needed for paxos instances acting as proposers.
*/
type AgreementState struct {
  highest_promised int          // highest proposal number promised in a prepare_ok reply
  highest_seen int              // highest proposal number seen
  accepted_proposal *Proposal   // highest numbered proposal that has been accepted
}


/*
Accepts the number of affirmative/ok replies from acceptors.
Returns a boolean indicating whether that is a majority of the pool of peer 
Paxos instances.
*/
func (px *Paxos) is_majority(ok_reply_count int) bool {
  if ok_reply_count <= (px.peer_count / 2) {
    return false
  }
  return true
}






//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()
    
  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }
  return false
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}


//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me
  // Your initialization code here.
  px.peer_count = len(peers)
  px.state = map[int]AgreementState{}

  
  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
