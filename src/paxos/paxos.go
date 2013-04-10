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
//import "runtime"
import "time"
import "fmt"
//import "math"
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
  state map[int]*AgreementState  // Key: Agreement instance number -> Value: Agreement State
  done map[string]int            // Key: Server name, Value: The most recently received value for that server's highest done value.
}

/*
The application wants paxos to start agreement instance with agreement_number and 
proposed value 'value'. Only agreement_numbers which the Paxos peers have not decided
to free from memory (i.e. above the minimum_done agreement which has been freed) are 
allowed.
If AgreementState for agreement_number not already created (by another proposer calling
Prepare_handler), it is initialized. 
Spawns a thread to act as the proposer to drive the agreement instance to a decision 
while the main thread handing the request returns right away.
The application will call Status() to find out if/when agreement is reached.
*/
func (px *Paxos) Start(agreement_number int, proposal_value interface{}) {
  px.mu.Lock()
  defer px.mu.Unlock()

  if agreement_number <= px.minimum_done_number() {
    return              // peers decided to free this agreement instance from memory
  }
  _, present := px.state[agreement_number]
  if !present {
    px.state[agreement_number] = px.make_default_agreementstate()
  }
  //fmt.Printf("Paxos Start (%s): agreement_number: %d, proposal_value: %v\n", short_name(px.peers[px.me], 7), agreement_number, proposal_value)

  // Spawn a thread to construct proposal and act as the proposer
  go px.proposer_role(agreement_number, proposal_value)

  return
}

/*
A client application of the Paxos library will call this method when it no longer
needs to call px.Status() for a particular agreement number. 
All replica servers keep track of Done calls they have received since any entries in
the px.state table for agreement instances below the minimum Done call agreement_number
can be deleted since all replica servers have indicated they no longer need to retrieve
the decision that was made for that agreement instance. 
Client replica server is done with all instances <= agreement_number.
*/
func (px *Paxos) Done(agreement_number int) {
  px.mu.Lock()
  defer px.mu.Unlock()
  // Update record of highest agreement_number marked done by the Paxos client.
  if agreement_number > px.done[px.peers[px.me]] {
    px.done[px.peers[px.me]] = agreement_number
  }
  //fmt.Printf("Paxos Done (%s): client_said: %d, my_h_done: %d\n", short_name(px.peers[px.me], 7), agreement_number, px.done[px.peers[px.me]])
}


/* 
Client wants the highest agreement instance number known to this peer. 
Returns -1 if no agreement instances are known (i.e. no AgreementState entries 
have been added to the px.state map)
*/
func (px *Paxos) Max() int {
  px.mu.Lock()
  defer px.mu.Unlock()

  max_agreement_instance_number := -1
  for agreement_number, _ := range px.state {
    if agreement_number > max_agreement_instance_number {
      max_agreement_instance_number = agreement_number
    }
  }
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
  px.mu.Lock()
  defer px.mu.Unlock()
  // Minimum agreement_number whose agreementstate is still maintained in px.state
  return px.minimum_done_number() + 1
}

/* The application wants to know whether this peer thinks an Agreement 
instance has been decided upon, and if so what the agreed value is.
Status() should just inspect the local peers AgreementState; it should not contact 
other Paxos peers.
If an agreement was reached, but all replica server clients of Paxos instances 
indicated they no longer needed the decided result, the result was deleted and 
false should be returned.
*/
func (px *Paxos) Status(agreement_number int) (bool, interface{}) {
  px.mu.Lock()
  defer px.mu.Unlock()

  if agreement_number <= px.minimum_done_number() {
    return false, nil   // Paxos clients all said ok to delete agreement result.
  }
  _, present := px.state[agreement_number]
  if present && px.state[agreement_number].decided {
    return true, px.state[agreement_number].accepted_proposal.Value
  } 
  return false, nil
}


/*
Paxos Proposer role drives an agreement instance to a decision/agreement. Should be
started as a separate thread.
Acts in the Paxos proposer role to first propose a proposal to the acceptors for 
agreement instance 'agreement_number' and if a majority reply with prepare_ok then
the proposer proceeds to phase 2. 
In phase 2, it sends an accept request to each of the Paxos peers with the proposal
that was sent in the prepare requests. If a majority of peers accept the proposal
then a decision has been made and Decided messages are broadcast to all peers. 
  If a majority was not reached during the prepare phase or the accept phase, execution
loops back the beginning of the proposer_role and a new (higher, but still unique)
proposal number is chosen and new proposal with this number created. The only ways
execution can leave the proposer role without this proposed_role thread making a 
decision is if 
* px.still_deciding returns false (indicating a competing proposer role caused a 
decision to be made)
* px.dead is true indicating that the test suite has marked the server as dead and 
thread execution should stop.
*/
func (px *Paxos) proposer_role(agreement_number int, proposal_value interface{}) {
  var done_proposing = false
  var majority_prepare bool
  var highest_number = -1
  var highest_accepted_proposal Proposal
  
  for !done_proposing && px.still_deciding(agreement_number) && !px.dead {
    
    // Generate the next number that is larger than highest_number
    proposal_number := px.next_proposal_number(agreement_number, highest_number)

    // Broadcast prepare request for agreement instance 'agreement_number' to Paxos acceptors.
    //fmt.Printf("Proposer [PrepareStage] (%s): agree_num: %d, prop: %d, val: %v\n", short_name(px.peers[px.me], 7), agreement_number, proposal_number, proposal_value)
    var proposal = Proposal{Number: proposal_number, Value: proposal_value}
    replies_from_prepare := px.broadcast_prepare(agreement_number, proposal)

    majority_prepare, highest_number, highest_accepted_proposal = px.evaluate_prepare_replies(replies_from_prepare)

    if !majority_prepare || !px.still_deciding(agreement_number) {
      //fmt.Printf("Proposer [PrepareStage] (%s): agree_num: %d, prop: %d, Majority not reached on prepare\n", short_name(px.peers[px.me], 7), agreement_number, proposal_number)    
      //runtime.Gosched()
      time.Sleep(time.Duration(rand.Intn(100)))
      continue
    }

    var empty_proposal = Proposal{}
    if highest_accepted_proposal != empty_proposal {
      // Accept request should have value v, where v is the value of highest-number among prepare replies
      proposal.Value = highest_accepted_proposal.Value
    }
    // Otherwise, the value may be kept at what the application calling px.Start requested.
    //fmt.Printf("Proposer [AcceptStage] (%s): agree_num: %d, prop: %d, val: %v\n", short_name(px.peers[px.me], 7), agreement_number, proposal_number, proposal.Value)
    replies_from_accept := px.broadcast_accept(agreement_number, proposal)
    majority_accept := px.evaluate_accept_replies(replies_from_accept)

    if majority_accept {
      // Broadcast decides
      //fmt.Printf("Proposer [DecisionReached] (%s): agree_num: %d, prop: %d, val: %v\n", short_name(px.peers[px.me], 7), agreement_number, proposal_number, proposal.Value)
      px.broadcast_decided(agreement_number, proposal)
      done_proposing = true
    } else {
      //fmt.Printf("Proposer [AcceptStage] (%s): agree_num: %d, Majority not reached on accept\n", short_name(px.peers[px.me], 7), agreement_number)    
      //runtime.Gosched()
      time.Sleep(time.Duration(rand.Intn(100)))
      continue
    }

  // End of proposing loop  
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

  _, present := px.state[agreement_number]
  if present {
    // continue accepting prepare requests
  } else {
    if !present && agreement_number > px.minimum_done_number() {
      // First hearing of agreement instance from some proposer
      px.state[agreement_number] = px.make_default_agreementstate()
    } else {
      //fmt.Println("Trying to prepare agreement that should not exist Error!!!")
      px.state[agreement_number] = px.make_default_agreementstate()
    }
  }

  if proposal_number > px.state[agreement_number].highest_promised {
    // Promise not to accept proposals numbered less than n
    px.state[agreement_number].set_highest_promised(proposal_number)
    reply.Prepare_ok = true
    reply.Number_promised = proposal_number
    reply.Accepted_proposal = px.state[agreement_number].accepted_proposal
    //fmt.Printf("Prepare_ok (%s): agreement_number: %d, proposal: %v\n", short_name(px.peers[px.me], 7), args.Agreement_number, reply.Accepted_proposal)
    return nil
  }
  reply.Prepare_ok = false
  reply.Number_promised = px.state[agreement_number].highest_promised
  reply.Accepted_proposal = px.state[agreement_number].accepted_proposal
  //fmt.Printf("Prepare_no (%s): agreement_number: %d, proposal: %v\n", short_name(px.peers[px.me], 7), args.Agreement_number, reply.Accepted_proposal)
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

  _, present := px.state[agreement_number]
  if present {
    // continue accepting prepare requests
  } else {
    if !present && agreement_number > px.minimum_done_number() {
      // First hearing of agreement instance from some proposer
      px.state[agreement_number] = px.make_default_agreementstate()
    } else {
      //fmt.Println("Trying to accept agreement that should not exist Error!!!")
      px.state[agreement_number] = px.make_default_agreementstate()
    }
  }

  if proposal.Number >= px.state[agreement_number].highest_promised {
    px.state[agreement_number].set_highest_promised(proposal.Number)
    px.state[agreement_number].set_accepted_proposal(proposal)
    reply.Accept_ok = true
    reply.Highest_done = px.done[px.peers[px.me]]
    //fmt.Printf("Accept_ok (%s): agree_num: %d, prop: %d, val: %v, h_done: %d\n", short_name(px.peers[px.me], 7), args.Agreement_number, args.Proposal.Number, proposal.Value, reply.Highest_done)
    return nil
  }
  reply.Accept_ok = false
  reply.Highest_done = px.done[px.peers[px.me]]
  //fmt.Printf("Accept_no (%s): agree_num: %d, prop: %d, h_done: %d\n", short_name(px.peers[px.me], 7), args.Agreement_number, args.Proposal.Number, reply.Highest_done)
  return nil
}


/*
Paxos library instances communicate with one another via exported RPC methods.
Accepts pointers to DecidedArgs and DecidedReply. Method will populate the DecidedReply
and return any errors.
*/
func (px *Paxos) Decided_handler(args *DecidedArgs, reply *DecidedReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  var agreement_number = args.Agreement_number
  var proposal = args.Proposal

  _, present := px.state[agreement_number]
  if !present {
    //fmt.Println("AgreementState should exist in DecidedHandler!!!")
    px.state[agreement_number] = px.make_default_agreementstate()
  } 
  // A leaner never learns that a value has been chosen unless it actually has been.
  px.state[agreement_number].set_decided(true)
  px.state[agreement_number].set_accepted_proposal(proposal)
  //fmt.Printf("Decided (%s): agree_num: %d, prop: %d, val: %v\n", short_name(px.peers[px.me], 7), args.Agreement_number, args.Proposal.Number, args.Proposal.Value)
  return nil
}

/*
Returns a reference to an AgreementState instance initialized with the correct default
values so that it is ready to be used in the px.state map.
The highest seen and highest promised are set to -1 and the proposal number is set to
the px.me index minux the number of peers since each time next_proposal_number is 
called, the number is incremented by the number of peers.
*/
func (px *Paxos) make_default_agreementstate() *AgreementState {
  initial_proposal_number := px.me - px.peer_count
  agrst := AgreementState{highest_promised: -1,
                          decided: false,
                          proposal_number: initial_proposal_number,
                          accepted_proposal: Proposal{},
                        } 
  return &agrst
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
To guarantee unique proposal numbers, the proposal number is initialized with the index 
of the Paxos instance minus the number of peer Paxos servers. On each call, the 
proposal number is incremented by the number of Paxos peers. This ensures that proposal
numbers used by Paxos peers are unique across the peer set and increasing.
Even if Start is called more than once, notice that the number to use is stored
for each paxos peer for each agreement state, meaning that even those instances will
have unique numbers - the proposal_number in agreement state is updated atomically
because of the lock taken out.
The highest_promised parameter is the highest promised number the proposer has seen 
so continue generating numbers as described above until one it found that is 
LARGER than highest_promised so the next prepare request has a chance of succeeding.
Note that the lock is kinda too coarse since we really
only need to lock a specific AgreementState, but this is fine.
*/
func (px *Paxos) next_proposal_number(agreement_number int, highest_promised int) int {
  // Formally only need to lock a specific AgreementState
  px.mu.Lock()
  defer px.mu.Unlock()

  //fmt.Printf("next_proposal state access (%s): %d, %v\n", short_name(px.peers[px.me], 7), agreement_number, px.state)
  current_proposal_number := px.state[agreement_number].proposal_number
  next_proposal_number := current_proposal_number + px.peer_count
  px.state[agreement_number].set_proposal_number(next_proposal_number)

  // for next_proposal_number <= highest_promised {
  //   // Repeat until a high enough proposal number is found.
  //   current_proposal_number := px.state[agreement_number].proposal_number
  //   next_proposal_number := current_proposal_number + px.peer_count
  //   px.state[agreement_number].set_proposal_number(next_proposal_number)
  // }
  // Multiplier is how many times px.peer_count should be added to increment proposal number
  //multiplier := (highest_promised - current_proposal_number)/

  return next_proposal_number
}


/*
Returns a boolean of whether an agreement instance is still in the process of being
decided, indicating that a proposer should continue trying to reach a decision.
*/
func (px *Paxos) still_deciding(agreement_number int) bool {
  px.mu.Lock()
  defer px.mu.Unlock()

  if agreement_number <= px.minimum_done_number() {
    return false        // Already decided and agreement instance memory freed
  }
  _, present := px.state[agreement_number]
  if present {
    if px.state[agreement_number].decided {
      return false     // already decided
    } else {
      return true      // agreement state is maintained, but no decision yet.
    }
  }
  return false        // agreement instance is not maintained on paxos instance
}


/*
Accepts an agreement instance agreement_number and a reference to a Proposal that should
be broadcast in a prepare request to all Paxos acceptors. Collects and returns a list 
of PrepareReply elements.
Does NOT mutate local px instance or take out any locks.
*/
func (px *Paxos) broadcast_prepare(agreement_number int, proposal Proposal) []PrepareReply {
  
  var replies_array = make([]PrepareReply, px.peer_count)    // declare and init
  for index, peer := range px.peers {
    if peer == px.peers[px.me] {
      // local_prepare can be used instead of RPC
      replies_array[index] = *(px.local_prepare(agreement_number, proposal.Number))
      continue
    }
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
func (px *Paxos) broadcast_accept(agreement_number int, proposal Proposal) []AcceptReply {
  
  var replies_array = make([]AcceptReply, px.peer_count)   // declare and init
  for index, peer := range px.peers {
    if peer == px.peers[px.me] {
      // local_accept can be used instead of RPC
      replies_array[index] = *(px.local_accept(agreement_number, proposal))
      continue                
    }
    args := &AcceptArgs{}       // declare and init struct with zero-valued fields. 
    args.Agreement_number = agreement_number
    args.Proposal = proposal
    var reply AcceptReply       // declare reply so ready to be modified by callee
    // Attempt to contact peer. No reply is equivalent to a vote no.
    call(peer, "Paxos.Accept_handler", args, &reply)
    replies_array[index] = reply
    px.update_done_entry(peer, reply.Highest_done)
  }
  return replies_array
}

/*
Accepts an agreement instance agreement_number and a reference to a Proposal that should
be broadcast in a decided request to all Paxos learners.
Does NOT mutate local px instance or take out any locks.
*/
func (px *Paxos) broadcast_decided(agreement_number int, proposal Proposal) {
  for _, peer := range px.peers {
    if peer == px.peers[px.me] {
      // local_decided can be used instead of RPC
      px.local_decided(agreement_number, proposal)
      continue                   // local_decided should be used instead of RPC
    }
    args := &DecidedArgs{}       // declare and init struct with zero-valued fields. 
    args.Agreement_number = agreement_number
    args.Proposal = proposal
    var reply DecidedReply       // declare reply so ready to be modified by callee
    // Attempt to contact peer. No reply is equivalent to a vote no.
    call(peer, "Paxos.Decided_handler", args, &reply)
  }
  return
}


/*
Simulates the actions of receiving a prepare RPC request but this method is called
locally as a function. Accepts agreement_number and proposal_number, the values that 
would be packaged inside PrepareArgs. 
Returns a pointer to an PrepareReply so all the replies (both local & RPC) can be 
evaluated together.
Since it simulates the Prepare_handler, it takes out a lock on the current Paxos
instance.
*/
func (px *Paxos) local_prepare(agreement_number int, proposal_number int) *PrepareReply {
  px.mu.Lock()
  defer px.mu.Unlock()
  var reply PrepareReply

  _, present := px.state[agreement_number]
  if present {
    // continue accepting prepare requests
  } else {
    if !present && agreement_number > px.minimum_done_number() {
      // First hearing of agreement instance from some proposer
      px.state[agreement_number] = px.make_default_agreementstate()
    } else {
      //fmt.Println("Trying to prepare agreement that should not exist Error!!!")
      px.state[agreement_number] = px.make_default_agreementstate()
    }
  }

  if proposal_number > px.state[agreement_number].highest_promised {
    // Promise not to accept proposals numbered less than n
    px.state[agreement_number].set_highest_promised(proposal_number)
    reply.Prepare_ok = true
    reply.Number_promised = proposal_number
    reply.Accepted_proposal = px.state[agreement_number].accepted_proposal
    //fmt.Printf("Prepare_ok (%s): agreement_number: %d, proposal: %v\n", short_name(px.peers[px.me], 7), agreement_number, reply.Accepted_proposal)
    return &reply
  }
  reply.Prepare_ok = false
  reply.Number_promised = px.state[agreement_number].highest_promised
  reply.Accepted_proposal = px.state[agreement_number].accepted_proposal
  //fmt.Printf("Prepare_no (%s): agreement_number: %d, proposal: %v\n", short_name(px.peers[px.me], 7), agreement_number, reply.Accepted_proposal)
  
  return &reply
}



/*
Simulates the actions of receiving an accept RPC request but this method is called
locally as a function. Accepts agreement_number and proposal, the values that would
be packaged inside AcceptArgs. 
Returns a pointer to an AcceptReply so all the replies (both local & RPC) can be 
evaluated together.
Since it simulates the Accept_handler, it takes out a lock on the current Paxos
instance.
*/
func (px *Paxos) local_accept(agreement_number int, proposal Proposal) *AcceptReply {
  px.mu.Lock()
  defer px.mu.Unlock()
  var reply AcceptReply

  _, present := px.state[agreement_number]
  if !present {
    //fmt.Println("AgreementState should exist in local_accept!!!")
    px.state[agreement_number] = px.make_default_agreementstate()
  } 

  if proposal.Number >= px.state[agreement_number].highest_promised {
    px.state[agreement_number].set_highest_promised(proposal.Number)
    px.state[agreement_number].set_accepted_proposal(proposal)
    reply.Accept_ok = true
    reply.Highest_done = px.done[px.peers[px.me]]
    //fmt.Printf("Accept_ok (%s): agree_num: %d, prop: %d, val: %v, h_done: %d\n", short_name(px.peers[px.me], 7), agreement_number, proposal.Number, proposal.Value, reply.Highest_done)
    return &reply
  }
  reply.Accept_ok = false
  reply.Highest_done = px.done[px.peers[px.me]]
  //fmt.Printf("Accept_no (%s): agree_num: %d, prop: %d, h_done: %d\n", short_name(px.peers[px.me], 7), agreement_number, proposal.Number, reply.Highest_done)
  return &reply
}


/*
Marks the agreement instance agreement_number as decided and sets the accepted 
proposal. This method is needed because the deaf proposer prohibits a proposer
from receiving RPC calls, in which case we must both broadcast decisions and 
update the local paxos instance.
*/
func (px *Paxos) local_decided(agreement_number int, proposal Proposal) {
  px.mu.Lock()
  defer px.mu.Unlock()

  _, present := px.state[agreement_number]
  if !present {
    //fmt.Println("AgreementState should exist in local_decided!!!")
    px.state[agreement_number] = px.make_default_agreementstate()
  } 
  // A leaner never learns that a value has been chosen unless it actually has been.
  px.state[agreement_number].set_decided(true)
  px.state[agreement_number].set_accepted_proposal(proposal)
}



/*
Accepts a peer string and the latest received highest done agreement number from
the acceptor. Check that this number is higher than the current highest done value 
stored in px.done for that server and if it is, update the px.done mapping
We technically only need to lock px.done, but locking the paxos instance is fine.
*/
func (px *Paxos) update_done_entry(paxos_peer string, highest_done int) {
  px.mu.Lock()
  defer px.mu.Unlock()
  
  if highest_done > px.done[paxos_peer] {
    px.done[paxos_peer] = highest_done
    //fmt.Printf("Update done (%s): Updated %s's h_done to %d\n", short_name(px.peers[px.me],7), short_name(paxos_peer,7), highest_done)
    px.attempt_free_state()
  }
}

/*
Computes the minimum agreement number marked as done among all the done agreement 
numbers reported back by peers and represented in the px.peers map.
Callee is responsible for taking out a lock on the paxos instance.
*/
func (px *Paxos) minimum_done_number() int {
  var min_done_number = px.done[px.peers[px.me]]
  for _, peers_done_number := range px.done {
    if peers_done_number < min_done_number {
      min_done_number = peers_done_number
    }
  }
  return min_done_number
}

/*
Computes the minimum agreement number in the values of px.done which indicates that
all Paxos peers have been told by their client server that all prior agreement states
are no longer needed. Thus, this Paxos instance may delete state corresponding to
agreements at or before the minimum agreement number in px.done.
Callee is reponsible for attaining a lock on the px.state map and px.done map.
*/
func (px *Paxos) attempt_free_state() {
  
  var min_done_number = px.minimum_done_number()

  //fmt.Printf("Free state (%s): min_done_val: %d\n", short_name(px.peers[px.me],7), min_done_number)
  for agreement_number, _ := range px.state {
    if agreement_number < min_done_number {
      //fmt.Println("DELETE", agreement_number, short_name(px.peers[px.me], 7))
      delete(px.state, agreement_number)
    }
  }

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
func (px *Paxos) evaluate_prepare_replies(replies_array []PrepareReply) (bool, int, Proposal){
  var ok_count = 0                  // number replies with prepare_ok = true
  var highest_number = -1           // highest number observed among replies Number_promised
  var highest_proposal Proposal     // highest numbered proposal reported as accepted by a peer, in a reply
  
  for _, reply := range replies_array {
    if reply.Prepare_ok {
      ok_count += 1
    }
    if reply.Number_promised > highest_number {
      highest_number = reply.Number_promised
      highest_proposal = reply.Accepted_proposal   // Actual Proposal or zero-valued Proposal
    }
  }

  return px.is_majority(ok_count), highest_number, highest_proposal


  //   /*Note, reply did not need to be prepare_ok for us to use the value in the highest
  //   numbered proposal an acceptor reports to have accepted*/
  //   if reply.Accepted_proposal.Number > highest_number
  //   if reply.Accepted_proposal != Proposal{} {
  //     if highest_proposal == Proposal{} {
  //       // No reply has yet reported a highest proposal accepted
  //       highest_proposal = reply.Accepted_proposal
  //     } else if reply.Accepted_proposal.Number > highest_proposal.Number {
  //       highest_proposal = reply.Accepted_proposal
  //     }
  //   }
  //   // Otherwise, Acceptor has not accepted a proposal.
  // }

  // if highest_proposal == nil {
  //   // No reply reported a highest accepted proposal
  //   return px.is_majority(ok_count), highest_number, false, nil
  // }
  // /* At least one Accepted_proposal was reported in a reply. The highest value among
  // these is reported to be used in the accept request.*/
  // return px.is_majority(ok_count), highest_number, true, highest_proposal.Value
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
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
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
