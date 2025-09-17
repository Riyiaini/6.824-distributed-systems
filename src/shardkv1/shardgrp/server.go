package shardgrp

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type KVEntry struct {
	Value   string
	Version rpc.Tversion
}

type Shard struct {
	KV      map[string]KVEntry
	Freezed bool
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	gid  tester.Tgid

	// Your code here
	mu     sync.Mutex
	Shards [shardcfg.NShards]Shard
	maxNum shardcfg.Tnum
}

func (kv *KVServer) DoOp(req any) any {
	// Your code here
	switch req := req.(type) {
	case rpc.GetArgs:
		return kv.ExecuteGetCmd(req)
	case rpc.PutArgs:
		return kv.ExecutePutCmd(req)
	case shardrpc.FreezeShardArgs:
		return kv.ExecuteFreezeCmd(req)
	case shardrpc.InstallShardArgs:
		return kv.ExecuteInstallCmd(req)
	case shardrpc.DeleteShardArgs:
		return kv.ExecuteDeleteCmd(req)
	default:
		log.Fatalf("unknown req type %T", req)
		return nil
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.Shards)
	e.Encode(kv.maxNum)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	if len(data) < 1 {
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var snapshot [shardcfg.NShards]Shard
	var maxnum shardcfg.Tnum
	if d.Decode(&snapshot) != nil || d.Decode(&maxnum) != nil {
		log.Fatal("Failed to restore state machine from snapshot")
	}
	kv.Shards = snapshot
	kv.maxNum = maxnum
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here
	if kv.Shards[shardcfg.Key2Shard(args.Key)].Freezed {
		reply.Err = rpc.ErrWrongGroup
		return
	}
	Err, rep := kv.rsm.Submit(*args)
	if Err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here
	if kv.Shards[shardcfg.Key2Shard(args.Key)].Freezed {
		reply.Err = rpc.ErrWrongGroup
		return
	}
	Err, rep := kv.rsm.Submit(*args)
	if Err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	reply.Err = rep.(rpc.PutReply).Err
}

// Freeze the specified shard (i.e., reject future Get/Puts for this
// shard) and return the key/values stored in that shard.
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// Your code here
	Err, rep := kv.rsm.Submit(*args)
	if Err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(shardrpc.FreezeShardReply)
}

// Install the supplied state for the specified shard.
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// Your code here
	Err, rep := kv.rsm.Submit(*args)
	if Err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(shardrpc.InstallShardReply)
}

// Delete the specified shard.
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// Your code here
	Err, rep := kv.rsm.Submit(*args)
	if Err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(shardrpc.DeleteShardReply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartShardServerGrp starts a server for shardgrp `gid`.
//
// StartShardServerGrp() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// Initialize all shards with empty KV maps
	for i := range kv.Shards {
		kv.Shards[i].KV = make(map[string]KVEntry)
		kv.Shards[i].Freezed = false
	}

	// Your code here
	kv.Restore(persister.ReadSnapshot())

	return []tester.IService{kv, kv.rsm.Raft()}
}
