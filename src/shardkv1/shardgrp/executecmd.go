package shardgrp

import (
	"bytes"

	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
)

func (kv *KVServer) ExecuteGetCmd(req rpc.GetArgs) rpc.GetReply {
	var reply rpc.GetReply

	kv.mu.Lock()
	shard := kv.Shards[int(shardcfg.Key2Shard(req.Key))]
	entry, ok := shard.KV[req.Key]
	kv.mu.Unlock()

	if !ok {
		reply.Err = rpc.ErrNoKey
		reply.Value = ""
		reply.Version = 0
	} else {
		reply.Value = entry.Value
		reply.Version = entry.Version
		reply.Err = rpc.OK
	}
	return reply
}

func (kv *KVServer) ExecutePutCmd(req rpc.PutArgs) rpc.PutReply {
	var reply rpc.PutReply

	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := kv.Shards[int(shardcfg.Key2Shard(req.Key))]
	entry, ok := shard.KV[req.Key]
	if !ok {
		if req.Version == 0 {
			shard.KV[req.Key] = KVEntry{
				Value:   req.Value,
				Version: 1,
			}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	} else {
		oVersion := entry.Version
		if req.Version != oVersion {
			reply.Err = rpc.ErrVersion
		} else {
			shard.KV[req.Key] = KVEntry{
				Value:   req.Value,
				Version: oVersion + 1,
			}
			reply.Err = rpc.OK
		}
	}
	return reply
}

func (kv *KVServer) ExecuteFreezeCmd(req shardrpc.FreezeShardArgs) shardrpc.FreezeShardReply {
	var reply shardrpc.FreezeShardReply

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if req.Num < kv.maxNum {
		reply.Err = rpc.ErrVersion
		reply.Num = kv.maxNum
		return reply
	} else if req.Num > kv.maxNum {
		kv.maxNum = req.Num
		reply.Num = req.Num
	}

	shard := &kv.Shards[int(req.Shard)]
	shard.Freezed = true

	kvpairs := make(map[string]KVEntry)
	for k, e := range shard.KV {
		// Make a copy of the KVEntry to avoid sharing references
		kvpairs[k] = KVEntry{
			Value:   e.Value,
			Version: e.Version,
		}
	}

	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)
	encoder.Encode(kvpairs)
	reply.State = w.Bytes()
	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) ExecuteInstallCmd(req shardrpc.InstallShardArgs) shardrpc.InstallShardReply {
	var reply shardrpc.InstallShardReply

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if req.Num < kv.maxNum {
		reply.Err = rpc.ErrVersion
		return reply
	} else if req.Num > kv.maxNum {
		kv.maxNum = req.Num
	}

	shard := &kv.Shards[int(req.Shard)]

	if shard.Freezed {
		shard.Freezed = false
	}

	var kvpairs map[string]KVEntry
	r := bytes.NewBuffer(req.State)
	d := labgob.NewDecoder(r)
	if d.Decode(&kvpairs) != nil {
		panic("Failed to Decode InstallShard state")
	}

	for k, e := range kvpairs {
		shard.KV[k] = e
	}
	reply.Err = rpc.OK
	return reply
}

func (kv *KVServer) ExecuteDeleteCmd(req shardrpc.DeleteShardArgs) shardrpc.DeleteShardReply {
	var reply shardrpc.DeleteShardReply

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if req.Num < kv.maxNum {
		reply.Err = rpc.ErrVersion
		return reply
	} else if req.Num > kv.maxNum {
		kv.maxNum = req.Num
	}

	shard := &kv.Shards[int(req.Shard)]
	if !shard.Freezed {
		panic("DeleteShard Error: shard not freezed")
	}

	for k := range shard.KV {
		delete(shard.KV, k)
	}

	reply.Err = rpc.OK
	return reply
}
