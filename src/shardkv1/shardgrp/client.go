package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	lastLeader int
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// Your code here
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}
	numServer := len(ck.servers)
	for {
		for srv := range numServer {
			idx := (srv + ck.lastLeader) % numServer
			ok := ck.clnt.Call(ck.servers[idx], "KVServer.Get", &args, &reply)
			if ok {
				if reply.Err == rpc.ErrWrongLeader {
					continue
				} else {
					ck.lastLeader = idx
					return reply.Value, reply.Version, reply.Err
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// Your code here
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}

	// log.Println("Clerk Put", key, value, version)
	retry := false
	numServer := len(ck.servers)
	for {
		for srv := range numServer {
			idx := (srv + ck.lastLeader) % numServer
			ok := ck.clnt.Call(ck.servers[idx], "KVServer.Put", &args, &reply)
			if ok {
				if reply.Err == rpc.ErrWrongLeader {
					continue
				}
				ck.lastLeader = idx
				if reply.Err == rpc.ErrVersion && retry {
					return rpc.ErrMaybe
				}
				return reply.Err
			}
		}
		retry = true
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// Your code here
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}
	reply := shardrpc.FreezeShardReply{}
	numServer := len(ck.servers)
	for {
		for srv := range numServer {
			idx := (srv + ck.lastLeader) % numServer
			ok := ck.clnt.Call(ck.servers[idx], "KVServer.FreezeShard", &args, &reply)
			if ok {
				if reply.Err == rpc.ErrWrongLeader {
					continue
				}
				ck.lastLeader = idx
				return reply.State, reply.Err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}
	reply := shardrpc.InstallShardReply{}
	numServer := len(ck.servers)
	for {
		for srv := range numServer {
			idx := (srv + ck.lastLeader) % numServer
			ok := ck.clnt.Call(ck.servers[idx], "KVServer.InstallShard", &args, &reply)
			if ok {
				if reply.Err == rpc.ErrWrongLeader {
					continue
				}
				ck.lastLeader = idx
				return reply.Err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// Your code here
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}
	reply := shardrpc.DeleteShardReply{}
	numServer := len(ck.servers)
	for {
		for srv := range numServer {
			idx := (srv + ck.lastLeader) % numServer
			ok := ck.clnt.Call(ck.servers[idx], "KVServer.DeleteShard", &args, &reply)
			if ok {
				if reply.Err == rpc.ErrWrongLeader {
					continue
				}
				ck.lastLeader = idx
				return reply.Err
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
