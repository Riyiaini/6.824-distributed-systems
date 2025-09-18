package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"sync/atomic"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	tester "6.5840/tester1"
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // set by Kill()

	// Your data here.
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// Your code here.
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
	backupCfg, _, err := sck.Get("backup-config")
	if err == rpc.ErrNoKey {
		return
	}
	cfg := shardcfg.FromString(backupCfg)
	sck.ChangeConfigTo(cfg)
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// Your code here
	stringCfg := cfg.String()
	sck.Put("config", stringCfg, 0)
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// Your code here.
	oldCfg, v, _ := sck.Get("config")

	old := shardcfg.FromString(oldCfg)
	if old.Num >= new.Num {
		return
	}

	_, ver, _ := sck.Get("backup-config")
	sck.Put("backup-config", new.String(), ver)

	for s, g := range old.Shards {
		if new.Shards[s] != g {

			servers := old.Groups[g]
			ck := shardgrp.MakeClerk(sck.clnt, servers)
			data, err := ck.FreezeShard(shardcfg.Tshid(s), new.Num)
			if err != rpc.OK {
				return // config out of date
			}

			nservers := new.Groups[new.Shards[s]]
			nck := shardgrp.MakeClerk(sck.clnt, nservers)
			err = nck.InstallShard(shardcfg.Tshid(s), data, new.Num)
			if err != rpc.OK {
				return
			}
			ck.DeleteShard(shardcfg.Tshid(s), new.Num)
		}
	}

	newCfg := new.String()
	sck.Put("config", newCfg, v)
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// Your code here.
	stringCfg, _, err := sck.Get("config")
	if err != rpc.OK {
		panic("Query: Get error")
	}
	cfg := shardcfg.FromString(stringCfg)
	return cfg
}

func (sck *ShardCtrler) Kill() {
	atomic.StoreInt32(&sck.killed, 1)
}

func (sck *ShardCtrler) Killed() bool {
	return atomic.LoadInt32(&sck.killed) == 1
}
