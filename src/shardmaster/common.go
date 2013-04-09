package shardmaster

import "fmt"

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

/*
Removes a replica group (RG) with the given non-zero GID. Be sure you've reassigned the
shards this RG was handling before removing it.
*/
func (self *Config) remove_replica_group(gid int64) {
  delete(self.Groups, gid)
}

/*
Assigns the shard with index shard_index to replica group gid, overwriting whatever RG it 
was previously assigned to, if any.
*/
func (self *Config) move(shard_index int, gid int64) {
  fmt.Println("Moving ", shard_index, " from ", self.Shards[shard_index], " to rg ", gid)
  self.Shards[shard_index] = gid
}

/*
Initializes a populates a map representing the replica group ids to number of shards 
maintained by that replica group mapping. Useful for determining the minimally and maximally
loaded replica groups.
*/
func (self *Config) build_load_table() map[int64]int {
  rg_loads := make(map[int64]int)
  for gid, _ := range self.Groups {
    rg_loads[gid] = 0                  // valid replica group gids are in the Groups map
  }
  for _, gid := range self.Shards {
    _, present := rg_loads[gid]        // Only increment for valid gids (i.e. in the table)
    if present {
      rg_loads[gid] += 1
    }
  }
  return rg_loads
}

/*
Considers all joined replica groups and determines a replica group responsible for a minimal
number of shards among its fellow replica groups. Returns the minimally loaded RG and number 
of shards of load on that replica replica.
If there are NO replica groups, will return int64 0 as the minimally loaded replica group
so that shards are switched back to gid 0.
*/
func (self *Config) minimally_loaded() (gid int64, load int) {
  rg_load := self.build_load_table()
  var min_load_gid int64 = 0        // repica gids are int64 (o is default non-valid gid)
  var min_load = NShards + 1        // bc 1 real RG w/ all shards preferable replica group 0

  for gid, shard_count := range rg_load {
    if shard_count < min_load {
      min_load = shard_count
      min_load_gid = gid
    }
  }
  return min_load_gid, min_load
}

/*
Considers all joined replica groups and determines a replica group responsible for a maximal
number of shards among its fellow replica groups. Returns the maximally loaded RG and the
number of shards of load on that replica group.
If there are NO replica groups, will return int64 0 as the maximally loaded replica group.
*/
func (self *Config) maximally_loaded() (gid int64, load int) {
  rg_load := self.build_load_table()
  var max_load_gid int64 = 0     // repica gids are int64 (o is default non-valid gid)
  var max_load = 0               // non-valid RG 0 is not actually responsible for shards.

  for gid, shard_count := range rg_load {
    if shard_count > max_load {
      max_load = shard_count
      max_load_gid = gid
    }
  }
  return max_load_gid, max_load
}

/*
Returns the int difference between the maximally and minimally loaded replica groups.
*/
func (self *Config) load_diff() int {
  _, min_load := self.minimally_loaded()
  _, max_load := self.maximally_loaded()
  return max_load - min_load
}

/*
Iterates through the Shards array, checking whether any are assigned to the non-valid RG with
gid 0. For each that is found, the shard is moved to the minimally loaded RG.
*/
func (self *Config) migrate_lonely_shards() {
  fmt.Println("Migrating lonely shards")
  for shard_index, gid := range self.Shards {
    if gid == 0 {                // non-valid RG, used before a Join has occurred.
      min_rg, _ := self.minimally_loaded()
      self.move(shard_index, min_rg)
    }
  }
  fmt.Println("Done migrating lonely shards")
  return
}

/*
Performs shard rebalancing using the given migration threshold (diff between max # shards and 
min # or shards) by moving shards from the maximally loaded RG to the minimally loaded RG in
order to minimize the number of shard transfers needed to reach a balanced state.
Requires threshold >= 1. Mutates the self Config object.
*/
func (self *Config) rebalance(threshold int) {
  if threshold < 1 {
    return
  }
  var max_rg, min_rg int64
  for self.load_diff() > threshold {
    max_rg, _ = self.maximally_loaded()
    min_rg, _ = self.minimally_loaded()
    // Choose 1 from the maximally loaded RG to move to the minimally loaded RG
    for shard_index, gid := range self.Shards {
      if gid == max_rg {
        self.move(shard_index, min_rg)
        break
      }
    }
  }
}



type Args interface {}

type Reply interface {

}

type Result interface {}

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
