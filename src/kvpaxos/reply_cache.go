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

/*
Returns the 'applied' bool field of the CacheEntry specified by the given
client_id and request_id. If no such CacheEntry exists in the KVCache, false 
is returned to indicate that the cache entry has not been applied to the
KVStorage instance.
*/
func (self *KVCache) was_applied(client_id int, request_id int) bool {
  if self.entry_exists(client_id, request_id) {
      cache_entry, _ := self.state[client_id][request_id]
      return cache_entry.applied
  }
  return false
}


func (self *KVCache) cached_reply(client_id int, request_id int) (Reply, error) {
  cache_entry, present := self.state[client_id][request_id]
  if present {
    return *cache_entry.reply, nil
  }
  return nil, errors.New("no cache entry")
}

/*
Attempts to create an entry in the KVCache state mapping client_id and request_id
to a zero-valued CacheEntry. Returns an error if a CacheEntry with the same
client_id and request_id already exists.
*/
func (self *KVCache) add_entry(client_id int, request_id int) error {
  if self.entry_exists(client_id, request_id) {
    return errors.New("cache entry already exists")
  }
  self.state[client_id] = map[int]*CacheEntry{}
  self.state[client_id][request_id] = &CacheEntry{}
  return nil
}


/*
Marks a CacheEntry at client_id, request_id as having been applied to the 
kvstorage by setting applied to be true. Returns an error if no CacheEntry is
found at the given (client_id, request_id) pair in the KVCache state map.
*/
func (self *KVCache) mark_as_applied(client_id int, request_id int) error {
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