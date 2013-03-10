package kvpaxos

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
)
type Err string

type PutArgs struct {
  // You'll have to add definitions here.
  Key string
  Value string
}

type PutReply struct {
  Err Err
}

type GetArgs struct {
  // You'll have to add definitions here.
  Key string
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
