package paxos

/*
Struct to represent a Proposal instance
*/

/* 
Representation of a Proposal which is what a proposer proposes to acceptors which 
has a unique number per Agreement instance and a value that the proposer wants to 
become the decided value for the agreement instance
*/
type Proposal struct {
  Number int               // proposal number (unqiue per Argeement instance)
  Value interface{}        // value of the Proposal
}
