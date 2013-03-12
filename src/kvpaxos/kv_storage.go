package kvpaxos

import "sync"
import "fmt"

/*
A KVStorage instance is responsible for storing the key/value pairs on a single 
kvpaxos instance of the Key/Value System. At any given time, the storage instance
represents the state of the key/value pairs up through operation 'operation_number'
Operations with agreement numbers higher than operation_number have not yet been
applied to the KVStorage instance representation.
*/
type KVStorage struct {
  mu sync.Mutex
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

/*
Applies an operation of type "GET_OP", "PUT_OP", or "NO_OP" to the key/value
state by increasing the operation number up to the operation number of the 
operation to be appled. Additionally, for PUT_OP, the state must be updated.
* Note that values will be retrieved to service client GET_OP requests 
separately - this function mutates the KVStorage state when neccessary.
Caller responsible for ensuring all previous operations are applied first.
*/  
func (self *KVStorage) apply_operation(operation Op, op_number int) {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.operation_number = op_number
	if operation.Kind == "PUT_OP" {
		self.state[operation.Key] = operation.Value
	} else {
		// "GET_OP" or "NO_OP"
	}
}
