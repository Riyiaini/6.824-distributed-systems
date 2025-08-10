package raft

import "6.5840/raftapi"

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogtTerm int // term of PrevLogIndex entry
	Entries      []logEntry
	LeaderCommit int
}
type AppendEntryReply struct {
	Term         int
	NextIndex    int
	ConflictTerm int
	Success      bool
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

	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	rf.sendToChannel(rf.heartbeatCh) // notify ticker goroutine

	// follower's log is shorter than leader's log
	lastIndex := rf.GetLastIndex()
	if args.PrevLogIndex > lastIndex {
		reply.NextIndex = lastIndex + 1
		reply.ConflictTerm = -1
		return
	}

	// follower's log conflicts with leader's at PrevLogIndex
	if cfTerm := rf.log[args.PrevLogIndex].Term; cfTerm != args.PrevLogtTerm {
		cfIndex := args.PrevLogIndex
		for cfIndex > 0 && rf.log[cfIndex].Term == cfTerm {
			cfIndex--
		}
		reply.NextIndex = cfIndex + 1
		reply.ConflictTerm = cfTerm
		return
	}

	// By this point, follower's log is consistent with leader's log before PrevLogIndex

	i, j := args.PrevLogIndex+1, 0
	for ; i <= lastIndex && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.log[i].Term != args.Entries[j].Term {
			break
		}
	}
	rf.log = rf.log[:i]
	rf.log = append(rf.log, args.Entries[j:]...)
	reply.ConflictTerm = -1
	reply.NextIndex = len(rf.log)

	reply.Success = true

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.GetLastIndex())
	}

	go rf.applyLogEntry()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntryArgs, reply *AppendEntryReply) {
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || args.Term < rf.currentTerm || reply.Term < rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
		return
	}

	// Appending entries success or the follower's log is shorter than the leader's log
	if reply.Success {
		rf.nextIndex[server] = reply.NextIndex
		rf.matchIndex[server] = reply.NextIndex - 1
	} else if reply.ConflictTerm == -1 {
		rf.nextIndex[server] = reply.NextIndex
	} else {
		idx := args.PrevLogIndex
		for ; idx > 0 && rf.log[idx].Term != reply.ConflictTerm; idx-- {
		}
		if idx == 0 {
			rf.nextIndex[server] = reply.NextIndex
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
