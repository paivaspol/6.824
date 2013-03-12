package kvpaxos

import "fmt"

/*
A KVStorage instance is responsible for storing the key/value pairs on a single 
kvpaxos instance of the Key/Value System. At any given time, the storage instance
represents the state of the key/value pairs up through operation 'operation_number'
Operations with agreement numbers higher than operation_number have not yet been
applied to the KVStorage instance representation.
*/
type KVStorage struct {
  //mu sync.Mutex
  state map[string]string      // key/value data storage
  operation_number int         // agreement number of latest applied operation 
}

func (self *KVStorage) get_operation_number() int {
	return self.operation_number
}

func (self *KVStorage) lookup(key string) (string, error) {
  value, present := self.state[key]
  if present {
    return value, nil
  }
  return "", fmt.Errorf("no key %s", key)
}

func (self *KVStorage) apply_operation(operation Op) {
	// LOCK
	fmt.Println(operation)
	self.operation_number += 1
}
