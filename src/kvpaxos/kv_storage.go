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

/*
Reads and returns the current operation number reflected in the state of the 
KVStorage key/value pairs.
*/
func (self *KVStorage) get_operation_number() int {
	self.mu.Lock()
	defer self.mu.Unlock()

	return self.operation_number
}

/*
Looks up the value associated with a given string key. Retuns a string value and
nil error if found or an empty string "" and an error if not found.
*/
func (self *KVStorage) lookup(key string) (string, error) {
	self.mu.Lock()
	defer self.mu.Unlock()

	value, present := self.state[key]
	if present {
	return value, nil
	}
	return "", fmt.Errorf("no key %s", key)
}

/*
Looks up the value associated with a given string key. Retuns a string value and
nil error if found or an empty string "" and an error if not found.
*/
func (self *KVStorage) lookup_lockless(key string) (string, error) {

	value, present := self.state[key]
	if present {
	return value, nil
	}
	return "", fmt.Errorf("no key %s", key)
}

/*
If the operation has not been applied according to the kvcache, 
1) the op of type "GET_OP", "PUT_OP", or "NO_OP" will be applied to the key/value 
state
2) the kvstorage internal operation number will be adjusted to that of the latest 
applied operation
3) the operation will be marked as having been applied in the kvcache



by increasing the operation number up to the operation number of the 
operation to be appled. Additionally, for PUT_OP, the state must be updated.
* Note that values will be retrieved to service client GET_OP requests 
separately - this function mutates the KVStorage state when neccessary.
Caller responsible for ensuring all previous operations are applied first.

!Note - if a lock was not taken out on the kvcache, it would be possible for some
process to change the state of the KVCache (ex. initially a cache entry exists,
but by the time we try to mark an operation as applied, the entry has been deleted)
*/  
func (self *KVStorage) apply_operation(op Op, op_number int, kvcache *KVCache) {
	self.mu.Lock()
	kvcache.mu.Lock()              // lock KVCache so read/writes are atomic
	defer kvcache.mu.Unlock()
	defer self.mu.Unlock()

	self.operation_number = op_number   // adjust KVStorage operation number

	// kvcache is created if it does not exist
	kvcache.add_entry_if_not_present(op.Client_id, op.Request_id)

	if kvcache.was_applied(op.Client_id, op.Request_id) {
		// operation is a duplicate, simply adjust operation_number
		fmt.Printf("Already applied requested op: %d (req: %d:%d)\n", op_number, op.Client_id, op.Request_id)
		return
	}	
	// Behavior if the operation has not already been applied.

	// apply operation to KVStorage, do nothing for GET_OP or NO_OP
	if op.Kind == "PUT_OP" {
		self.state[op.Key] = op.Value
	}

	kvcache.mark_as_applied(op.Client_id, op.Request_id)
	var reply Reply
	if op.Kind == "PUT_OP" {
		reply = self.reply_from_put(op)
	} else {
		// GET_OP or NO_OP (no client should be requesting a NO_OP)
		reply = self.reply_from_get(op)
	}
	fmt.Printf("Reply op: %d (req: %d:%d) %v\n", op_number, op.Client_id, op.Request_id, reply)
	kvcache.record_reply(op.Client_id, op.Request_id, reply)
	return	
}

/*
Prepares the appropriate reply based on the given state of the kvstore and the given
op. Note that it is the caller's responsibility to ensure that the kvstore is in 
a state where it is appropriate to get the reply from the operation. Typically, this
means the operation has just been applied to the kvstore (self.apply_operation) so
now it is safe to cache a reply.
Caller is responsible for obtaining a lock on the kvstore
*/
func (self *KVStorage) reply_from_put(op Op) *PutReply {
	put_reply := PutReply{}
	put_reply.Err = OK
	return &put_reply
}


func (self *KVStorage) reply_from_get(op Op) *GetReply {
	if op.Kind == "GET_OP" || op.Kind == "NO_OP" {
		get_reply := GetReply{}
		value, error := self.lookup_lockless(op.Key)
		if error == nil {
			get_reply.Err = OK
			get_reply.Value = value
			return &get_reply
		} 
		get_reply.Err = ErrNoKey
		get_reply.Value =""
		return &get_reply
	}
	panic("attempted to create reply for invalid operation")
}