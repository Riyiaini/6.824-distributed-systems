package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

type logEntry struct {
	Term    int
	Command interface{}
}

type State int

const (
	// Raft server states
	Follower = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers:
	currentTerm int
	votedFor    int
	log         []logEntry

	// Volatile state on all servers:
	commitIndex int
	lastApplied int

	// Volatile state on leaders:
	nextIndex  []int
	matchIndex []int

	// Other state:
	state       State                 // Follower, Candidate, Leader
	applyCh     chan raftapi.ApplyMsg // channel to send apply messages to the service
	heartbeatCh chan struct{}
	convertCh   chan struct{}
	winElectCh  chan struct{} // channel to notify when state changes to Leader
	voteCount   int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var cTerm int
	var vtFor int
	var logs []logEntry
	if d.Decode(&cTerm) == nil && d.Decode(&vtFor) == nil && d.Decode(&logs) == nil {
		rf.currentTerm = cTerm
		rf.votedFor = vtFor
		rf.log = logs
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// return the index of the last log entry.
func (rf *Raft) GetLastIndex() int {
	return len(rf.log) - 1 // last log entry is at index len(log)-1
}

func (rf *Raft) GetLastTerm() int {
	return rf.log[rf.GetLastIndex()].Term // last log entry's term
}

func heartbeatTimeout() time.Duration {
	return time.Duration(100) * time.Millisecond
}

func electionTimeout() time.Duration {
	ms := 300 + (rand.Int63() % 200)
	return time.Duration(ms) * time.Millisecond
}

func (rf *Raft) isLogUpToDate(lastLogIndex, lastLogTerm int) bool {
	// Check if the log is up to date.
	if lastLogTerm > rf.GetLastTerm() {
		return true
	} else if lastLogTerm == rf.GetLastTerm() {
		return lastLogIndex >= rf.GetLastIndex()
	}
	return false
}

func (rf *Raft) sendToChannel(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (rf *Raft) convertToFollower(term int) {

	state := rf.state
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.voteCount = 0

	if state != Follower {
		rf.sendToChannel(rf.convertCh) // notify heartbeat goroutine
	}
}

func (rf *Raft) convertToCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		return
	}
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.voteCount = 1 // vote for itself
	rf.persist()

	rf.broadcastRequestVote()
}

func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Candidate {
		return
	}

	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	lastIndex := rf.GetLastIndex() + 1
	for i := range rf.peers {
		rf.nextIndex[i] = lastIndex
		rf.matchIndex[i] = 0
	}

	rf.broadcastHeartbeat()
}

// braodcast append entries to all followers.
func (rf *Raft) broadcastHeartbeat() {

	if rf.state != Leader {
		return
	}
	for i := range rf.peers {
		if i != rf.me {
			args := &AppendEntryArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogtTerm: rf.log[rf.nextIndex[i]-1].Term,
				Entries:      make([]logEntry, len(rf.log)-rf.nextIndex[i]),
				LeaderCommit: rf.commitIndex,
			}
			copy(args.Entries, rf.log[rf.nextIndex[i]:])
			go rf.sendAppendEntries(i, args, &AppendEntryReply{})
		}
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, -1, false
	}

	term := rf.currentTerm
	rf.log = append(rf.log, logEntry{
		Term:    term,
		Command: command,
	})
	rf.persist()

	return rf.GetLastIndex(), term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	// DPrintf("peer %d ticker started", rf.me)
	for !rf.killed() {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()
		switch state {
		case Leader:
			select {
			case <-rf.convertCh: // convert to follower
			case <-time.After(heartbeatTimeout()):
				rf.mu.Lock()
				rf.broadcastHeartbeat()
				rf.mu.Unlock()
			}
		case Follower:
			select {
			case <-rf.heartbeatCh:
			case <-time.After(electionTimeout()):
				rf.convertToCandidate()
			}
		case Candidate:
			select {
			case <-rf.convertCh: // convert to follower
			case <-rf.winElectCh:
				rf.convertToLeader()
			case <-time.After(electionTimeout()):
				rf.convertToCandidate()
			}
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		currentTerm: 0,
		votedFor:    -1,
		log:         make([]logEntry, 0), // fisrt index is 1
		commitIndex: 0,
		lastApplied: 0,
		nextIndex:   make([]int, len(peers)),
		matchIndex:  make([]int, len(peers)),
		state:       Follower,
		applyCh:     applyCh,
		heartbeatCh: make(chan struct{}),
		convertCh:   make(chan struct{}),
		winElectCh:  make(chan struct{}),
		voteCount:   0,
		dead:        0,
	}

	// Your initialization code here (3A, 3B, 3C).
	rf.log = append(rf.log, logEntry{0, nil})
	for i := range rf.peers {
		rf.nextIndex[i], rf.matchIndex[i] = rf.GetLastIndex()+1, 0
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
