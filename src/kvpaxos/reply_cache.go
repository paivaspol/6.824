package kvpaxos

//import "fmt"
import "sync"
import "errors"



/*
Structure for managing mappings of request identifiers (client_id, request_id) to 
replies to ensure that a duplicate request (due to network unpredictability) still
results in the client receiving a notification of the operation performed.
*/
type KVCache struct {
  mu sync.Mutex
  state map[int]map[int]*CacheEntry    // cache data storage
}


type CacheEntry struct {
  applied bool      // whether request operation already applied to kvstore (don't apply incoming duplicates)
  // cached Reply or nil if operation handled by a different kvserver and discovered at this local server via Paxos instance log.
  reply *Reply
}

/*
Returns boolean of whether or not a reply has been returned for a particular 
request
*/
func (self *KVCache) entry_exists(client_id int, request_id int) bool {
  _, present := self.state[client_id][request_id]
  if present {
    return true
  }
  return false
}


func (self *KVCache) was_applied(client_id int, request_id int) (bool, error) {
  cache_entry, present := self.state[client_id][request_id]
  if present {
    return cache_entry.applied, nil
  }
  return false, errors.New("no cache entry")
}


func (self *KVCache) cached_reply(client_id int, request_id int) (Reply, error) {
  cache_entry, present := self.state[client_id][request_id]
  if present {
    return *cache_entry.reply, nil
  }
  return nil, errors.New("no cache entry")
}


func (self *KVCache) mark_as_applied(client_id int, request_id int) error {
  self.mu.Lock()
  defer self.mu.Unlock()

  if self.entry_exists(client_id, request_id) {
    cache_entry, _ := self.state[client_id][request_id]
    cache_entry.applied = true
    return nil
  }
  return errors.New("no cache entry")
}


func (self *KVCache) record_reply(client_id int, request_id int, reply *Reply) error {
  self.mu.Lock()
  defer self.mu.Unlock()

  if self.entry_exists(client_id, request_id) {
    cache_entry, _ := self.state[client_id][request_id]
    cache_entry.reply = reply
    return nil
  }
  return errors.New("no cache entry")
}