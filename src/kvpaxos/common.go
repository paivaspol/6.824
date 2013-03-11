package kvpaxos

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
)
type Err string

type Request interface {
  get_client_id() int
  get_request_id() int
}

type Reply interface {
  is_ok() bool
}

// PutArgs and PutReply
///////////////////////////////////////////////////////////////////////////////

type PutArgs struct {
  Client_id int         // client_id
  Request_id int        // request_id unique per client
  Key string            // assignment specified keys will be strings
  Value string          // assignment specified values will be strings
}

func (self *PutArgs) get_client_id() int {
  return self.Client_id
}

func (self *PutArgs) get_request_id() int {
  return self.Request_id
}

func (self *PutArgs) get_key() string {
  return self.Key
}

func (self *PutArgs) get_value() string {
  return self.Value
}



type PutReply struct {
  Err Err               // string representation of an error. "OK" means no error.
}

func (self *PutReply) is_ok() bool {
  return self.Err == OK
}


// GetArgs and GetReply
///////////////////////////////////////////////////////////////////////////////

type GetArgs struct {
  // You'll have to add definitions here. 
  Client_id int         // client_id
  Request_id int        // request_id unique per client    
  Key string
}

func (self *GetArgs) get_client_id() int {
  return self.Client_id
}

func (self *GetArgs) get_request_id() int {
  return self.Request_id
}

func (self *GetArgs) get_key() string {
  return self.Key
}


type GetReply struct {
  Err Err               // string representation of an error. "OK" means no error.
  Value string          // value corresponding to key or "" if ErrNoKey
}

func (self *GetReply) is_ok() bool {
  return self.Err == OK
}



/*
In the tests used by test_test.go, the tail ends of server names are usually unique 
enough to identify the server in printouts.
*/
func short_name(server_name string, end int) string {
  if len(server_name) < end {
    return server_name
  }
  return server_name[len(server_name)-end:]
}
