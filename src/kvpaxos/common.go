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
  is_error() bool
}

// PutArgs and PutReply
///////////////////////////////////////////////////////////////////////////////

type PutArgs struct {
  Client_id int         // client_id
  Request_id int        // request_id unique per client
  Key string
  Value string
}

func (self *PutArgs) get_client_id() int {
  return self.Client_id
}

func (self *PutArgs) get_request_id() int {
  return self.Request_id
}


type PutReply struct {
  Err Err
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


type GetReply struct {
  Err Err
  Value string
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
