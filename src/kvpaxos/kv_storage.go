package kvpaxos

import "fmt"


type KVStorage struct {
  //mu sync.Mutex
  state map[string]string      // key/value data storage
  operation_number int         // agreement number of latest applied operation 
}

func (self *KVStorage) lookup(key string) (string, error) {
  value, present := self.state[key]
  if present {
    return value, nil
  }
  return "", fmt.Errorf("no key %s", key)
}
