package paxos

/*
Strucs for RPC communication between Paxos Peer instances.
*/


type PrepareArgs struct {
	Sequence_number int         // sequence number of the agreement instance
	Proposal_number int         // proposal number of the proposal
}

type PrepareReply struct {
	Prepare_ok bool             // was prepare ok'ed?
	Number_promised int         // promise not to accept any more proposals less than n
	Accepted_proposal *Proposal // highest numbered proposal that has been accepted
}


type AcceptArgs struct {
	Sequence_number int         // sequence number of the agreement instance
	N_proposal int              // proposal number     
	V_proposal interface{}      
	/* value of highest numbered accepted proposal among the prepare_ok reponses or the 
	value the proposer wishes to propose if responses returned no proposals */
}


type AcceptReply struct {
	accept_ok bool              // whether the accept proposal request was accepted
	N_accepted int              // number of highest accepted proposal
}