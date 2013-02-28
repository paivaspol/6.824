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
  // Proposer
  proposal_number int           // Proposal number propser is using currently.
  // Acceptor
  highest_promised int          // highest proposal number promised in a prepare_ok reply
  highest_seen int              // highest proposal number seen
  accepted_proposal *Proposal   // highest numbered proposal that has been accepted
}

/*
Sets the value for the highest_promised field of an AgreementState instance
Caller is responsible for attaining a lock on the AgreementState in some way
before calling
*/
func (agrst *AgreementState) set_highest_promised(new_highest_promised int) {
  agrst.highest_promised = new_highest_promised
}

/*
Sets the value for the accepted_proposal field of an AgreementState instance
Caller is responsible for attaining a lock on the AgreementState in some way
before calling
*/
func (agrst *AgreementState) set_accepted_proposal(proposal *Proposal) {
  agrst.accepted_proposal = proposal
}


type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int                         // index into peers[]
  peer_count int                 // Number of Paxos Peers
  state map[int]*AgreementState  // Key: Agreement instance number -> Value: Agreement State
  done map[string]int            // Key: Server name, Value: The most recently received value for that server's highest done value.
}

/*
The application wants paxos to start agreement instance with agreement_number and 
proposed value 'value'.
Add an AgreementState to the Paxos instance state, which indicates that negotiating the
agreement agreement_number has begun. Spawn a thread to act as the proposer to drive
the agreement instance to a decision while the main thread handing the request returns
right away.
The application will call Status() to find out if/when agreement is reached.
*/
func (px *Paxos) Start(agreement_number int, proposal_value interface{}) {
  px.mu.Lock()
  defer px.mu.Unlock()

  // Add AgreementState to Paxos instance state, negotiating known to have begun.
  _, present := px.state[agreement_number]
  fmt.Printf("Paxos Start (%s): agreement_number: %d, proposal_value: %v\n", short_name(px.peers[px.me], 7), agreement_number, proposal_value)
  if !present {
    fmt.Println("Initializing Agreement entry during Start")
    px.state[agreement_number] = &AgreementState{highest_promised: -1, highest_seen: -1}   
  } else {
    fmt.Println("State already present!")
  }

  // propose RPC call send to all peers
  //proposal_number := px.state[agreement_number].next_proposal_number()
  proposal_number := px.first_number()
  
  // Spawn a thread to construct proposal and act as the proposer
  go px.proposer_role(agreement_number, proposal_number, proposal_value)

  return
}

/*
A client application of the Paxos library will call this method when it no longer
needs to call px.Status() for a particular agreement number. 
All replica servers keep track of Done calls they have received since any entries in
the px.state table for agreement instances below the minimum Done call agreement_number
can be deleted since all replica servers have indicated they no longer need to retrieve
the decision that was made for that agreement instance.
rs have received a 
Client replica server is done with all instances <= agreement_number.
*/
func (px *Paxos) Done(agreement_number int) {
  px.mu.Lock()
  defer px.mu.Unlock()

  // Update the record the highest agreement_number that has been marked as done by the client of this Paxos instance.
  if agreement_number > px.done[px.peers[px.me]] {
    px.done[px.peers[px.me]] = agreement_number
  }
}

/* 
The client application would like to know the highest agreement instance number 
known to this peer. Returns -1 if no agreement instances are known (i.e. no 
AgreementState entries have been added to the px.state map)
*/
func (px *Paxos) Max() int {
  max_agreement_instance_number := -1
  for index, _ := range px.state {
    if index > max_agreement_instance_number {
      max_agreement_instance_number = index
    }
  }
  fmt.Println(max_agreement_instance_number)
  return max_agreement_instance_number
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
  var min_done int
  for _, val := range px.done {
    if val < min_done {
      min_done = val
    }
  }
  return min_done + 1
}

/* The application wants to know whether this peer thinks an Agreement 
instance has been decided upon, and if so what the agreed value is.
Status() should just inspect the local peers state; it should not contact 
other Paxos peers.
*/
func (px *Paxos) Status(sequence_number int) (bool, interface{}) {
  agreement_state, present := px.state[sequence_number]
  if present && agreement_state.accepted_proposal != nil {
    return true, agreement_state.accepted_proposal.Value
  }
  return false, nil
}


/*
Paxos Proposer role drives an agreement instance to a decision/agreement. Should be
started as a separate thread.
Acts in the Paxos proposer role to first propose a proposal to the acceptors for 
agreement instance 'agreement_number' and if a majority reply with prepare_ok then
the proposer proceeds to phase 2. 
In phase 2, it sends an accept request for the
proposal for agreement instance 'sequence_number' to all the acceptors.
?????
*/
func (px *Paxos) proposer_role(agreement_number int, proposal_number int, proposal_value interface{}) {

  // Broadcast prepare request for agreement instance 'agreement_number' to Paxos acceptors.
  var proposal = &Proposal{Number: proposal_number, Value: proposal_value}
  fmt.Printf("Propser [PrepareStage] (%s): agreement_number: %d, p_number: %d, p_value: %v\n", short_name(px.peers[px.me], 7), agreement_number, proposal_number, proposal_value)
  replies_from_prepare := px.broadcast_prepare(agreement_number, proposal)

  majority, high_val_found, high_val := px.evaluate_prepare_replies(replies_from_prepare)
  //fmt.Println(majority, high_val_found, high_val)

  if !majority {
    fmt.Printf("Propser [PrepareStage] (%s): agreement_number: %d, p_number: %d, Majority not reached\n", short_name(px.peers[px.me], 7), agreement_number, proposal_number)
    return
  }

  if high_val_found {
    // Accept request should have value v, where v is the value of highest-number among prepare replies
    proposal.Value = high_val
  }
  // Otherwise, the value may be kept at what the application calling px.Start requested.
  fmt.Printf("Propser [AcceptStage] (%s): agreement_number: %d, p_number: %d, p_value: %v\n", short_name(px.peers[px.me], 7), agreement_number, proposal_number, proposal_value)
  replies_from_accept := px.broadcast_accept(agreement_number, proposal)
  //fmt.Println(replies_from_accept)
  majority_accept := px.evaluate_accept_replies(replies_from_accept)
  //fmt.Println(majority_accept)
  if majority_accept {
    fmt.Printf("Propser [DecisionReached] (%s): agreement_number: %d, p_number: %d, p_value: %v\n", short_name(px.peers[px.me], 7), agreement_number, proposal_number, proposal_value)
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

  var agreement_number = args.Agreement_number
  var proposal_number = args.Proposal_number
  //fmt.Printf("Prepare_handler (%s): agreement_number: %d, p_number: %v\n", short_name(px.peers[px.me], 7), args.Agreement_number, args.Proposal_number)
 
  _, present := px.state[agreement_number]
  if !present {
    fmt.Println("Initializing map entry")
    px.state[agreement_number] = &AgreementState{highest_promised: -1, highest_seen: -1}
  } 

  if proposal_number > px.state[agreement_number].highest_promised {
    // Promise not to accept proposals numbered less than n
    px.state[agreement_number].set_highest_promised(proposal_number)
    reply.Prepare_ok = true
    reply.Number_promised = proposal_number
    reply.Accepted_proposal = px.state[agreement_number].accepted_proposal
    fmt.Printf("Prepare_ok (%s): agreement_number: %d, p_number: %v\n", short_name(px.peers[px.me], 7), args.Agreement_number, args.Proposal_number)
    return nil
  }
  reply.Prepare_ok = false
  fmt.Printf("Prepare_no (%s): agreement_number: %d, p_number: %v\n", short_name(px.peers[px.me], 7), args.Agreement_number, args.Proposal_number)
  return nil
}


/*
Paxos library instances communicate with one another via exported RPC methods.
Accepts pointers to PrepageArgs and PrepareReply. Method will populate the PrepareReply
and return any errors.
*/
func (px *Paxos) Accept_handler(args *AcceptArgs, reply *AcceptReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  var agreement_number = args.Agreement_number
  var proposal = args.Proposal
  //fmt.Printf("Accept_handler (%s): agreement_number: %d, p_number: %d, p_value: %v\n", short_name(px.peers[px.me], 7), args.Agreement_number, args.Proposal.Number, args.Proposal.Value)

  _, present := px.state[agreement_number]
  if !present {
    fmt.Println("Initializing map entry")
    px.state[agreement_number] = &AgreementState{highest_promised: -1, highest_seen: -1}
  } 

  if proposal.Number >= px.state[agreement_number].highest_promised {
    // 
    px.state[agreement_number].set_highest_promised(proposal.Number)
    px.state[agreement_number].set_accepted_proposal(proposal)
    reply.Accept_ok = true
    fmt.Printf("Accept_ok (%s): agreement_number: %d, p_number: %d, p_value: %v\n", short_name(px.peers[px.me], 7), args.Agreement_number, args.Proposal.Number, args.Proposal.Value)
    return nil
  }
  reply.Accept_ok = false
  fmt.Printf("Accept_no (%s): agreement_number: %d, p_number: %d\n", short_name(px.peers[px.me], 7), args.Agreement_number, args.Proposal.Number)
  return nil
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


/*
To guarantee unique proposal numbers, the first proposal number used in any Agreement
instance should be the px.me number. A unique sequence of proposal numbers is 
generated from this by passing the previous_number to the px.next_number(int) method.
*/
func (px *Paxos) first_number() int {
  return px.me
}

/*
Accepts the previous proposal number a proposer_role used and increments it to
return the next proposal number that should be used. The returned proposal int
is guaranteed to be higher than the previous proposal number and also unique among 
proposal numbers used by any peer replica server. Works because peer replica servers 
have unique px.me values which can be used to generate unique sequences.
*/
func (px *Paxos) next_number(previous_number int) int {
  return previous_number + px.peer_count
}


/*
Checks whether an agreement instance with 'agreement_number' has an accepted proposal
which means a decision has been reached and the value in the porposal will always be
the decided value for this agreement instance.
Note that just because a peer instance has not received an accepted_proposal does not
mean that the Paxos peers have not reached a decision.
*/
func (px *Paxos) has_decision(agreement_number int) bool {
  agreement_state, present := px.state[agreement_number]
  if !present {
    return false     // Paxos instance has not started or observed ngeotation for this agreement. 
  } else if agreement_state.accepted_proposal != nil {
    return true      // A proposal has been accepted
  }
  return false       // Negotiation started, but this Paxos instance has not set an accepted_proposal
}


/*
Accepts an agreement instance agreement_number and a reference to a Proposal that should
be broadcast in a prepare request to all Paxos acceptors. Collects and returns a list 
of PrepareReply elements.
Does NOT mutate local px instance or take out any locks.
*/
func (px *Paxos) broadcast_prepare(agreement_number int, proposal *Proposal) []PrepareReply {
  
  var replies_array = make([]PrepareReply, px.peer_count)    // declare and init
  for index, peer := range px.peers {
    args := &PrepareArgs{}       // declare and init struct with zero-valued fields. 
    args.Agreement_number = agreement_number
    args.Proposal_number = proposal.Number
    var reply PrepareReply       // declare reply so ready to be modified by callee
    // Attempt to contact peer. No reply is equivalent to a vote no.
    call(peer, "Paxos.Prepare_handler", args, &reply)
    replies_array[index] = reply
  }
  return replies_array
}

/*
Accepts an agreement instance agreement_number and a reference to a Proposal that should
be broadcast in an accept request to all Paxos acceptors. Collects and returns a list 
of AcceptReply elements.
Does NOT mutate local px instance or take out any locks.
*/
func (px *Paxos) broadcast_accept(agreement_number int, proposal *Proposal) []AcceptReply {
  
  var replies_array = make([]AcceptReply, px.peer_count)   // declare and init
  for index, peer := range px.peers {
    args := &AcceptArgs{}       // declare and init struct with zero-valued fields. 
    args.Agreement_number = agreement_number
    args.Proposal = proposal
    var reply AcceptReply       // declare reply so ready to be modified by callee
    // Attempt to contact peer. No reply is equivalent to a vote no.
    call(peer, "Paxos.Accept_handler", args, &reply)
    replies_array[index] = reply
  }
  return replies_array
}


/*
Evaluates the replies sent back by Paxos acceptors in response to prepare requests
and checks to see if a majority of them were prepare_ok responses. Also compares all
of the 'Accepted_proposal' fields in replies (representing the highest numbered 
proposal the acceptor has accepted) and determines among these the highest numbered
proposal -> the value of this proposal should be used in the subsequent accept
requests.
Returns bool of whether a majority was reached or not, a boolean indicating whether
a highest accepted proposal was reported back by any acceptors, and the value from the highest
numbered proposal reported by any acceptor in a reply to a prepare request. If no 
highest accepted proposal was reported back, the reported highest_value is simply
an empty interface instance.
Does NOT mutate local px instance or take out any locks.
*/
func (px *Paxos) evaluate_prepare_replies(replies_array []PrepareReply) (majority bool, found_highest_value bool, highest_value interface{}){
  var ok_count = 0                  // number replies with prepare_ok = true
  var highest_proposal *Proposal    // highest numbered proposal reported as accepted by a peer, in a reply
  for _, reply := range replies_array {
    if reply.Prepare_ok {
      ok_count += 1
    }
    /*Note, reply did not need to be prepare_ok for us to use the value in the highest
    numbered proposal an acceptor reports to have accepted*/
    if reply.Accepted_proposal != nil {
      if highest_proposal == nil {
        // No reply has yet reported a highest proposal accepted
        highest_proposal = reply.Accepted_proposal
      } else if reply.Accepted_proposal.Number > highest_proposal.Number {
        highest_proposal = reply.Accepted_proposal
      }
    }
    // Otherwise, Acceptor has not accepted a proposal.
  }

  if highest_proposal == nil {
    // No reply reported a highest accepted proposal
    return px.is_majority(ok_count), false, nil
  }
  /* At least one Accepted_proposal was reported in a reply. The highest value among
  these is reported to be used in the accept request.*/
  return px.is_majority(ok_count), true, highest_proposal.Value
}


/*
Evaluates the replies sent back by Paxos acceptors in response to accept requests
and checks to see if a majority of them were accept_ok responses.
Returns bool of whether a majority was reached or not.
Does NOT mutate local px instance or take out any locks.
*/
func (px *Paxos) evaluate_accept_replies(replies_array []AcceptReply) (majority bool){
  var ok_count = 0                  // number replies with accept_ok = true
  for _, reply := range replies_array {
    if reply.Accept_ok {
      ok_count += 1
    }
  }
  return px.is_majority(ok_count)
}






/*
In the tests used by test_test.go, the tail ends of server names are usually unique 
enough to identify the server in printouts.
*/
func short_name(server_name string, end int) string {
  if len(server_name) < end {
    return server_name
  }
  return server_name[len(server_name)-end:]
}






// Make/Kill Paxos instances, RPC call helper
///////////////////////////////////////////////////////////////////////////////


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
  px.state = map[int]*AgreementState{}
  px.done = map[string]int{}
  for _, peer := range px.peers {
    // First agreement instance agreement_number is 0. Initially clients have not marked it done.
    px.done[peer] = -1     
  }
  
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
