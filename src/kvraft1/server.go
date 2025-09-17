package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVEntry struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu sync.Mutex
	KV map[string]KVEntry
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here

	switch req := req.(type) {
	case rpc.GetArgs:
		var reply rpc.GetReply
		kv.mu.Lock()
		entry, ok := kv.KV[req.Key]
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
	case rpc.PutArgs:
		var reply rpc.PutReply
		kv.mu.Lock()
		defer kv.mu.Unlock()

		entry, ok := kv.KV[req.Key]
		if !ok {
			if req.Version == 0 {
				kv.KV[req.Key] = KVEntry{
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
				kv.KV[req.Key] = KVEntry{
					Value:   req.Value,
					Version: oVersion + 1,
				}
				reply.Err = rpc.OK
			}
		}
		return reply
	default:
		log.Fatalf("unknown req type %T", req)
		return nil
	}
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.KV)
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {

	// Your code here
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var snapshot map[string]KVEntry
	if d.Decode(&snapshot) != nil {
		log.Fatal("Failed to restore state machine from snapshot")
	}
	kv.KV = snapshot

}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)

	Err, rep := kv.rsm.Submit(*args)
	if Err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	*reply = rep.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)

	Err, rep := kv.rsm.Submit(*args)
	if Err == rpc.ErrWrongLeader {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	reply.Err = rep.(rpc.PutReply).Err
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

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	kv.KV = make(map[string]KVEntry)
	kv.Restore(persister.ReadSnapshot())

	return []tester.IService{kv, kv.rsm.Raft()}
}
