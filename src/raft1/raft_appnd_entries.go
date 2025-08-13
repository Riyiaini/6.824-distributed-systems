package raft

import (
	"time"

	"6.5840/raftapi"
)

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogtTerm int // term of PrevLogIndex entry
	Entries      []logEntry
	LeaderCommit int
}
type AppendEntryReply struct {
	Term    int
	XIndex  int
	XTerm   int
	Success bool
}

func (rf *Raft) applyLogEntry() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i,
		}
		rf.applyCh <- msg
		rf.lastApplied = i
	}
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	persistStateChanged := false
	defer func() {
		if persistStateChanged {
			rf.persist()
		}
	}()

	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
		persistStateChanged = true
	}

	reply.Term = rf.currentTerm
	rf.sendToChannel(rf.heartbeatCh) // notify ticker goroutine

	// follower's log is shorter than leader's log
	lastIndex := rf.GetLastIndex()
	if args.PrevLogIndex > lastIndex {
		reply.XIndex = lastIndex + 1
		reply.XTerm = -1
		return
	}

	// follower's log conflicts with leader's at PrevLogIndex
	if cfTerm := rf.log[args.PrevLogIndex].Term; cfTerm != args.PrevLogtTerm {
		cfIndex := args.PrevLogIndex
		for cfIndex > 0 && rf.log[cfIndex].Term == cfTerm {
			cfIndex--
		}
		reply.XIndex = cfIndex + 1
		reply.XTerm = cfTerm
		return
	}

	// By this point, follower's log before PrevLogIndex is consistent with leader's

	i, j := args.PrevLogIndex+1, 0
	for ; i <= lastIndex && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.log[i].Term != args.Entries[j].Term {
			break
		}
	}
	rf.log = rf.log[:i]
	rf.log = append(rf.log, args.Entries[j:]...)
	if i <= lastIndex || j < len(args.Entries) {
		persistStateChanged = true
	}
	reply.XTerm = -1
	reply.XIndex = len(rf.log)

	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.GetLastIndex())
	}

	go rf.applyLogEntry()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) {
	ok := false
	for nTry := 0; !ok && nTry < 5; ok, nTry = rf.peers[server].Call("Raft.AppendEntries", args, reply), nTry+1 {
		time.Sleep(60 * time.Millisecond)
	}
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || args.Term < rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
		rf.persist()
		return
	}

	if reply.Success {
		// Appending entries success
		rf.nextIndex[server] = reply.XIndex
		rf.matchIndex[server] = reply.XIndex - 1
	} else if reply.XTerm == -1 {
		// log is too short
		rf.nextIndex[server] = reply.XIndex
	} else {
		idx := args.PrevLogIndex
		for ; idx > 0 && rf.log[idx].Term != reply.XTerm; idx-- {
		}
		if idx == 0 {
			rf.nextIndex[server] = reply.XIndex
		} else {
			rf.nextIndex[server] = idx + 1
			rf.matchIndex[server] = idx
		}
	}

	for n := rf.GetLastIndex(); n > rf.commitIndex; n-- {
		count := 1
		if rf.log[n].Term == rf.currentTerm {
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= n {
					count++
				}
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = n
			go rf.applyLogEntry()
			break
		}
	}
}
