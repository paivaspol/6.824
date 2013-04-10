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

/*
Removes a replica group (RG) with the given non-zero GID. Remove a replica group before 
calling the reassign_shards method to move the shards from the removed replica group to the
remaining minimally loaded replica group
*/
func (self *Config) remove_replica_group(gid int64) {
  self.Num += 1
  delete(self.Groups, gid)
}

/*
Moves a specified shard to a specified gid by reassigning the shard to that gid, overwriting
whichever replica group gid the shard was previously assigned to.
Does NOT rebalance as the user has explicitly requested a specific shard movement. Adding or
removing other replica groups can undo the move though since they rebalance.
*/
func (self *Config) explicit_move(shard_index int, gid int64) {
  self.Num += 1
  self.move(shard_index, gid)
}

/*
Assigns the shard with index shard_index to replica group gid, overwriting whatever RG it 
was previously assigned to, if any.
Assumes that the specified gid is an allowed gid.
*/
func (self *Config) move(shard_index int, gid int64) {
  //fmt.Println("Moving ", shard_index, " from ", self.Shards[shard_index], " to rg ", gid)
  self.Shards[shard_index] = gid
}

/*
Initializes and populates a map representing the replica group ids to number of shards 
maintained by that replica group mapping. Useful for determining the minimally and maximally
loaded replica groups.
*/
func (self *Config) get_loads() map[int64]int {
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
  loads := self.get_loads()
  var min_load_gid int64 = 0        // repica gids are int64 (o is default non-valid gid)
  var min_load = NShards + 1        // bc 1 real RG w/ all shards preferable replica group 0

  for gid, shard_count := range loads {
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
  loads := self.get_loads()
  var max_load_gid int64 = 0     // repica gids are int64 (o is default non-valid gid)
  var max_load = 0               // non-valid RG 0 is not actually responsible for shards.

  for gid, shard_count := range loads {
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
If there are no valid replica groups, the shards are 'moved' to replica group 0 since 
minimally_loaded will return gid 0, so they are still assigned to a nonvalid replica group.
*/
func (self *Config) promote_shards_from_nonvalids() {
  for shard_index, gid := range self.Shards {
    if gid == 0 {                // non-valid RG, used before a Join has occurred.
      min_rg, _ := self.minimally_loaded()
      self.move(shard_index, min_rg)
    }
  }
  return
}

/*
Iterates through the Shards array, checking whether any shards were assigned to the passed
gid. Any that are found are moved to a minimally loaded replica group in preparation for 
the removal of the specified replica group.
*/
func (self *Config) reassign_shards(bad_gid int64) {
  for shard_index, gid := range self.Shards {
    if gid == bad_gid {
      min_rg, _ := self.minimally_loaded()
      self.move(shard_index, min_rg)
    }
  }
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

// Communication Types
///////////////////////////////////////////////////////////////////////////////

type Args interface {}

type JoinArgs struct {
  GID int64        // unique replica group ID
  Servers []string // group server ports
}

type LeaveArgs struct {
  GID int64
}

type MoveArgs struct {
  Shard int
  GID int64
}

type QueryArgs struct {
    Num int // desired config number
}

type NoopArgs struct {}



type Reply interface {}

type JoinReply struct {
}

type LeaveReply struct {
}

type MoveReply struct {
}

type QueryReply struct {
  Config Config
}

type Result interface {}

