package config

import (
	"fmt"
	"hash/fnv"

	"github.com/BurntSushi/toml"
)

// Struct to store configuration of a single shard
type Shard struct {
	Name    string
	Idx     int
	Address string
	close   bool
}

// Struct to store multiple shard
type Config struct {
	Shards []Shard
}

// Parse provided toml config file into Config struct
func ParseFile(filename string) (Config, error) {
	var c Config
	if _, err := toml.DecodeFile(filename, &c); err != nil {
		return Config{}, err
	}
	return c, nil
}

// Struct to store overall config of shards based on toml file
type Shards struct {
	Count  int
	CurIdx int
	Addrs  map[int]string
}

// Parse shards
func ParseShards(shards []Shard, curShardName string) (*Shards, error) {
	// get length of total shards provided in toml file
	shardCount := len(shards)

	// Assign current shard as -1 that will be treated as Error is not updated to any value greater than 0
	shardIdx := -1

	// make a mapping for shard id to their shard address provided in toml file
	addrs := make(map[int]string)

	// iterate on array of shards
	for _, s := range shards {
		// check for duplicate shards in the parsed shards data
		if _, ok := addrs[s.Idx]; ok {
			return nil, fmt.Errorf("duplicate shard index: %d", s.Idx)
		}

		// assign address in the map with respect to its shard id
		addrs[s.Idx] = s.Address

		// if shard name is same as current shard name, update current shardIdx
		if s.Name == curShardName {
			shardIdx = s.Idx
		}
	}

	// Check if all shards have been parsed or not
	for i := 0; i < shardCount; i++ {
		// if a index of shard is missed, throw an error
		if _, ok := addrs[i]; !ok {
			return nil, fmt.Errorf("shard %d is not found", i)
		}
	}

	// If current shardIdx is not updated from -1, throw an error as shard was not parsed correctl
	if shardIdx < 0 {
		return nil, fmt.Errorf("shard %q was not found", curShardName)
	}

	return &Shards{
		Addrs:  addrs,
		Count:  shardCount,
		CurIdx: shardIdx,
	}, nil
}

// This is a very important function and is responsible finding index of shard to which a key should be redirect for get/set calls
// DANGER - THIS IS RESPONSIBLE FOR SHARDING FUNCTIONALITY. DO NOT CHANGE IF THERE IS EXISTING DATA IN ANY DATABASE
func (s *Shards) Index(key string) int {
	h := fnv.New64()
	h.Write([]byte(key))
	return int(h.Sum64() % uint64(s.Count))
}
