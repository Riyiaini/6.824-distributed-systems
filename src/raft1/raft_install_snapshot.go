package raft

import (
	"time"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int // index of snapshot
	LastIncludedTerm  int // term of LastIncludedIndex entry
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) WriteSnapshot(snapshot []byte, data []byte, offset int) []byte {
	newlLen := max(offset+len(data), len(snapshot))
	newSnapshot := make([]byte, newlLen)
	copy(newSnapshot, snapshot)
	copy(newSnapshot[offset:], data)
	return newSnapshot
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	shouldPersist := false
	defer func() {
		if shouldPersist {
			rf.persist(rf.snapshotBuffer)
		}
	}()

	if args.Term < rf.currentTerm || args.LastIncludedIndex < rf.lastSnapshotIndex {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
		shouldPersist = true
	}

	rf.sendToChannel(rf.heartbeatCh)

	rf.snapshotBuffer = rf.WriteSnapshot(rf.snapshotBuffer, args.Data, args.Offset)
	if !args.Done || (args.LastIncludedIndex == rf.lastSnapshotIndex && rf.log[0].Term == args.LastIncludedTerm) {
		// snapshot sending is not complete or receiver's snapshot is already up to date
		reply.Term = rf.currentTerm
		return
	}

	rf.bufferWriteDone = true
	shouldPersist = true
	reply.Term = rf.currentTerm

	if args.LastIncludedIndex > rf.lastSnapshotIndex {
		logIdx := rf.ToLogIndex(args.LastIncludedIndex)
		if logIdx >= len(rf.log) {
			rf.log = append([]logEntry{}, logEntry{Term: args.LastIncludedTerm})
		} else if rf.log[logIdx].Term != args.LastIncludedIndex {
			rf.log = append([]logEntry{}, logEntry{Term: args.LastIncludedTerm})
		} else {
			rf.log = rf.log[logIdx:]
			rf.log[0].Command = nil
		}

	}

	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)

	rf.applyCond.Signal()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := false
	for nTry := 0; !ok && nTry < 5; ok, nTry = rf.peers[server].Call("Raft.InstallSnapshot", args, reply), nTry+1 {
		time.Sleep(60 * time.Millisecond)
	}
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
		rf.persist(nil)
		return
	}

	if rf.state != Leader || args.Term < rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
}
