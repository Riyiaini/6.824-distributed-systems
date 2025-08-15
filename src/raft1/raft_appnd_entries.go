package raft

import (
	"time"
)

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int // term of PrevLogIndex entry
	Entries      []logEntry
	LeaderCommit int
}
type AppendEntryReply struct {
	Term       int
	XIndex     int
	XTerm      int
	Success    bool
	LastCommit int // last commit index in the follower's log, for leader to update its commitIndex
	// must add this to pass test 2B
}

func (rf *Raft) AppendEntries(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	persistStateChanged := false
	defer func() {
		if persistStateChanged {
			rf.persist(nil)
		}
	}()

	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.LeaderCommit < rf.commitIndex {
		reply.LastCommit = rf.commitIndex
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

	logIndex := rf.ToLogIndex(args.PrevLogIndex)
	if logIndex < 0 {
		// force leader to send snapshot
		reply.XIndex = 0
		reply.XTerm = -1
		return
	}
	// follower's log conflicts with leader's at PrevLogIndex
	if cfTerm := rf.log[logIndex].Term; cfTerm != args.PrevLogTerm {
		if logIndex == 0 {
			// follower's last snapshot term conflicts with leader's at PrevLogIndex
			// force leader to send snapshot
			reply.XIndex = 0
			reply.XTerm = -1
			return
		}
		for logIndex > 0 && rf.log[logIndex].Term == cfTerm {
			logIndex--
		}
		reply.XIndex = rf.ToOriginalIndex(logIndex + 1)
		reply.XTerm = cfTerm
		return
	}

	// By this point, follower's log before PrevLogIndex is consistent with leader's

	i, j := logIndex+1, 0
	for ; i < len(rf.log) && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.log[i].Term != args.Entries[j].Term {
			break
		}
	}
	if i < len(rf.log) || j < len(args.Entries) {
		persistStateChanged = true
	}
	rf.log = rf.log[:i]
	rf.log = append(rf.log, args.Entries[j:]...)

	reply.XTerm = -1
	reply.XIndex = rf.ToOriginalIndex(len(rf.log))

	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.GetLastIndex())
		rf.applyCond.Signal()
	}
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
		rf.persist(nil)
		return
	}

	if reply.Success {
		// Appending entries success
		rf.nextIndex[server] = reply.XIndex
		rf.matchIndex[server] = reply.XIndex - 1
	} else if reply.XTerm == -1 {
		rf.nextIndex[server] = reply.XIndex
	} else {
		idx := rf.ToLogIndex(args.PrevLogIndex)
		for ; idx > 0 && rf.log[idx].Term != reply.XTerm; idx-- {
		}
		if idx == 0 {
			rf.nextIndex[server] = reply.XIndex
		} else {
			rf.nextIndex[server] = rf.ToOriginalIndex(idx + 1)
			rf.matchIndex[server] = rf.ToOriginalIndex(idx)
		}
	}

	commitIdx := rf.ToLogIndex(rf.commitIndex)
	for n := len(rf.log) - 1; n > commitIdx; n-- {
		count := 1
		idx := rf.ToOriginalIndex(n)
		if rf.log[n].Term == rf.currentTerm {
			for i := range rf.peers {
				if i != rf.me && rf.matchIndex[i] >= idx {
					count++
				}
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = idx
			rf.applyCond.Signal()
			return
		}
	}

	if rf.commitIndex < reply.LastCommit {
		rf.commitIndex = reply.LastCommit
		rf.applyCond.Signal()
	}
}
