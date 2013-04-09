package shardmaster

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(gid, servers) -- replica group gid is joining, give it some shards.
// Leave(gid) -- replica group gid is retiring, hand off all its shards.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// A GID is a replica group ID. GIDs must be uniqe and > 0.
// Once a GID joins, and leaves, it should never join again.
//
// Please don't change this file.
//

const NShards = 10

type Config struct {
  Num int // config number
  Shards [NShards]int64 // gid
  Groups map[int64][]string // gid -> servers[]
}

/*
Returns a new Config which has the same values as the Config instance copy was
called on.
*/
func (self *Config) copy() Config {
  var config = Config{Num: self.Num,
                      Shards: self.Shards,
                      Groups: make(map[int64][]string)}
  for key, value := range self.Groups {
    config.Groups[key] = value
  }
  return config
}

/*
Adds a new replica group (RG) with the given non-zero GID and the slice of server ports
of the replica servers (i.e. shardkv instances) that compose the replica group.
*/
func (self *Config) add_replica_group(gid int64, servers []string) {
  self.Num += 1
  self.Groups[gid] = servers
}






type Args interface {}

type Reply interface {

}

type JoinArgs struct {
  GID int64        // unique replica group ID
  Servers []string // group server ports
}

type JoinReply struct {
}

type LeaveArgs struct {
  GID int64
}

type LeaveReply struct {
}

type MoveArgs struct {
  Shard int
  GID int64
}

type MoveReply struct {
}

type QueryArgs struct {
    Num int // desired config number
}

type QueryReply struct {
  Config Config
}
