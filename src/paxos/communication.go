package paxos

/*
Strucs for RPC communication between Paxos Peer instances.
*/


type PrepareArgs struct {
	Agreement_number int         // sequence number of the agreement instance
	Proposal_number int         // proposal number of the proposal
}

type PrepareReply struct {
	Prepare_ok bool             // was prepare ok'ed?
	Number_promised int         // promise not to accept any more proposals less than n
	Accepted_proposal *Proposal // highest numbered proposal that has been accepted
}


type AcceptArgs struct {
	Sequence_number int         // sequence number of the agreement instance
	Proposal *Proposal          // Proposal to contend to be the decided proposal
}


type AcceptReply struct {
	Accept_ok bool              // whether the accept proposal request was accepted
}