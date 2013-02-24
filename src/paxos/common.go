package paxos


type AgreementState struct {
	n_prepare int            // number of highest prepare seen
	n_accept int             // number of highest numbered accept
 	v_accept interface{}     // accepted value (every higher numbered accepted proposal has same v)
}


type PrepareArgs struct {
	Sequence_number int      // sequence number of the agreement instance
	N_proposal int           // proposal number of the proposal
}

type PrepareReply struct {
	Prepare_ok bool             // whether the prepare was accepted
	N_highest_accepted int      // number of highest accepted proposal
	V_accepted interface{}      // value of highest accepted proposal
}


type AcceptArgs struct {
	Sequence_number int      // sequence number of the agreement instance
	N_proposal int           // proposal number     
	V_proposal interface{}      
	/* value of highest numbered accepted proposal among the prepare_ok reponses or the 
	value the proposer wishes to propose if responses returned no proposals */
}


type AcceptReply struct {
	accept_ok bool              // whether the accept proposal request was accepted
	N_accepted int              // number of highest accepted proposal
}