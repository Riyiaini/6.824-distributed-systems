package rsm

import (
	"log"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Req      any
	SubmitId int
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	appliedChs   map[int]chan any
	lastSnapshot int
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		appliedChs:   make(map[int]chan any),
		lastSnapshot: 0,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.Reader()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	op := Op{Req: req, SubmitId: rsm.me}

	index, _, isLeader := rsm.rf.Start(op)

	if isLeader {
		rsm.mu.Lock()
		ch, _ := rsm.getChannel(index)
		rsm.mu.Unlock()
		defer func() {
			rsm.mu.Lock()
			close(ch)
			delete(rsm.appliedChs, index)
			rsm.mu.Unlock()
		}()
		time0 := time.Now()
		for time.Since(time0).Seconds() < 5 {
			select {
			case rep := <-ch:
				return rpc.OK, rep

			case <-time.After(500 * time.Millisecond):
				_, isLeader := rsm.rf.GetState()
				if !isLeader {
					return rpc.ErrWrongLeader, nil
				}
			}
		}
	}
	return rpc.ErrWrongLeader, nil // i'm dead, try another server.
}

func (rsm *RSM) getChannel(index int) (chan any, bool) {
	ch, ok := rsm.appliedChs[index]
	if !ok {
		rsm.appliedChs[index] = make(chan any, 1)
		ch = rsm.appliedChs[index]
	}

	return ch, ok
}

func (rsm *RSM) Reader() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)
			index := msg.CommandIndex
			rep := rsm.sm.DoOp(op.Req)

			if index-rsm.lastSnapshot >= rsm.maxraftstate {
				rsm.lastSnapshot = index
				snapshot := rsm.sm.Snapshot()
				rsm.rf.Snapshot(index, snapshot)
			}

			if op.SubmitId != rsm.me {
				continue
			}

			rsm.mu.Lock()
			ch, exists := rsm.getChannel(index)
			if !exists {
				close(ch)
				delete(rsm.appliedChs, index)
				rsm.mu.Unlock()
				continue
			}
			rsm.mu.Unlock()

			ch <- rep
		} else if msg.SnapshotValid {
			rsm.sm.Restore(msg.Snapshot)
		} else {
			log.Fatal("InValid apply message")
		}
	}
}
