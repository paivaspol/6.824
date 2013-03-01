package paxos


/* 
Represents the state stored by a Paxos library instance per Agreement instance.
'number_promised' is the number returned in prepare_ok replies to promise not to 
accept any more proposals numbered less than 'number_promised'
accepted_proposal is the highest numbered proposal that has been accepted.
decided indicates that a decision about this agreement instance was reached and while
the accepted_proposal can change, its value never will.
*/
type AgreementState struct {
	decided bool
	// Proposer
	proposal_number int           // Proposal number propser is using currently.
	// Acceptor
	highest_promised int          // highest proposal number promised in a prepare_ok reply
	accepted_proposal *Proposal   // highest numbered proposal that has been accepted
}

/*
Sets the value for the highest_promised field of an AgreementState instance
Caller is responsible for attaining a lock on the AgreementState in some way
before calling.
*/
func (agrst *AgreementState) set_highest_promised(new_highest_promised int) {
	agrst.highest_promised = new_highest_promised
}

/*
Sets the value for the accepted_proposal field of an AgreementState instance
Caller is responsible for attaining a lock on the AgreementState in some way
before calling.
*/
func (agrst *AgreementState) set_accepted_proposal(proposal *Proposal) {
	agrst.accepted_proposal = proposal
}

/*
Sets the value for the proposal_number field of the AgreementState instance
Caller is responsible for attaining a lock of the AgreementState in some way
before calling.
*/
func (agrst *AgreementState) set_proposal_number(number int) {
	agrst.proposal_number = number
}

/*
Sets the value for the decided field of the AgreementState instance
Caller is responsible for attaining a lock of the AgreementState in some way
before calling.
*/
func (agrst *AgreementState) set_decided(decided bool) {
	agrst.decided = decided
}

