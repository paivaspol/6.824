package kvpaxos

//import "fmt"
import "errors"



/*
Structure for managing mappings of request identifiers (client_id, request_id) to 
replies to ensure that a duplicate request (due to network unpredictability) still
results in the client receiving a notification of the operation performed.
*/
type ReplyCache struct {
  //mu sync.Mutex
  state map[int]map[int]*Reply    // cache data storage
}

func (self *ReplyCache) entry_exists(client_id int, request_id int) bool {
  _, present := self.state[client_id][request_id]
  if present {
    return true
  }
  return false
}

func (self *ReplyCache) logged_reply(client_id int, request_id int) (*Reply, error) {
  logged_reply, present := self.state[client_id][request_id]
  if present {
    return logged_reply, nil
  }
  return nil, errors.New("no logged reply found")
}

// func (self *ReplyCache) add_entry(client_id int, request_id int, reply *Reply) error {
//   self.internal_log[client_id][request_id] = reply
//   if _, present := self.internal_log[client_id][request_id] == reply {
//     return nil
//   }
//   return errors.New("failed add record to RequestLog")
// }