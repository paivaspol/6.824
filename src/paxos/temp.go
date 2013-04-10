package paxos

// func (px *Paxos) local_accept(agreement_number int, proposal Proposal) *AcceptReply {
//   px.mu.Lock()
//   defer px.mu.Unlock()
//   var reply AcceptReply

//   _, present := px.state[agreement_number]
//   if !present {
//     //fmt.Println("AgreementState should exist in local_accept!!!")
//     px.state[agreement_number] = px.make_default_agreementstate()
//   } 

//   if proposal.Number >= px.state[agreement_number].highest_promised {
//     px.state[agreement_number].set_highest_promised(proposal.Number)
//     px.state[agreement_number].set_accepted_proposal(proposal)
//     reply.Accept_ok = true
//     reply.Highest_done = px.done[px.peers[px.me]]
//     output_debug(fmt.Sprintf("Accept_ok (%s): agree_num: %d, prop: %d, val: %v, h_done: %d", short_name(px.peers[px.me], 7), agreement_number, proposal.Number, proposal.Value, reply.Highest_done))
//     return &reply
//   }
//   reply.Accept_ok = false
//   reply.Highest_done = px.done[px.peers[px.me]]
//   output_debug(fmt.Sprintf("Accept_no (%s): agree_num: %d, prop: %d, h_done: %d", short_name(px.peers[px.me], 7), agreement_number, proposal.Number, reply.Highest_done))
//   return &reply
// }