package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"labrpc"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//

type logEntry struct {
	Index   int
	Term    int
	Command interface{}
}

var (
	electionTimeoutMin = 300
	electionTimeoutMax = 500
	heartbeatInterval  = 100
)

var (
	roleLeader    = 1
	roleCandidate = 2
	roleFollower  = 3
)

type peerMsg struct {
	svcMeth string
	args    interface{}
	reply   interface{}
}

type peer struct {
	nextIndex       int
	matchIndex      int
	end             *labrpc.ClientEnd
	lastCommunicate time.Time
}

func (p *peer) Call(svcMeth string, args interface{}, reply interface{}) bool {
	p.lastCommunicate = time.Now()
	return p.end.Call(svcMeth, args, reply)
}

type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	peers     []*peer    // RPC end points of all peers
	persister *Persister // Object to hold this peer's persisted state
	me        int        // this peer's index into peers[]
	leader    int

	//Persistent state on all servers
	currentTerm int
	votedFor    *int
	log         []logEntry

	//Volatile state on all servers:
	commitIndex int
	lastApplied int

	voteNum int

	role int

	electionTimer *time.Timer

	//eventCh chan interface{}

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func (rf *Raft) resetElectionTimer() {

	timeout := time.Duration(randBetween(electionTimeoutMin, electionTimeoutMax)) * time.Millisecond

	if nil == rf.electionTimer {
		rf.electionTimer = time.AfterFunc(timeout, rf.onElectionTimeout)
	} else {
		rf.electionTimer.Reset(timeout)
	}

}

func (rf *Raft) stopElectionTimer() bool {
	if nil != rf.electionTimer {
		return rf.electionTimer.Stop()
	} else {
		return true
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	//var term int
	//var isleader bool
	// Your code here (2A).
	//return term, isleader
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.leader == rf.me
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type RequestAppendEntrysArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []logEntry
}

type RequestAppendEntrysReply struct {
	Term    int
	Success bool
}

func (rf *Raft) onElectionTimeout() {
	fmt.Println("onElectionTimeout")
	rf.becomeCandidate()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == roleLeader && args.Term > rf.currentTerm {
		//leader发现更大的term,becomeFollower
		rf.becomeFollower()
	}

	if args.Term < rf.currentTerm {
		//收到更小的term,拒绝同时把自身term返回
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	} else if rf.votedFor != nil && *rf.votedFor != args.CandidateId {
		//已经投过票了
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	lastLogTerm := 0
	lastLogIndex := 0
	lastEntry := rf.getLastEntry()
	if nil != lastEntry {
		lastLogTerm = lastEntry.Index
		lastLogTerm = lastEntry.Term
	}

	if lastLogTerm == args.LastLogTerm {
		if lastLogIndex > args.LastLogIndex {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
	} else if lastLogTerm > args.LastLogTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	//可以支持
	rf.votedFor = &args.CandidateId
	rf.currentTerm = args.Term
	reply.Term = rf.currentTerm
	reply.VoteGranted = true

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	fmt.Println("sendRequestVote")
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) getEntryByIndex(index int) *logEntry {
	if index > 0 && index < len(rf.log) {
		return &rf.log[index]
	} else {
		return nil
	}
}

func (rf *Raft) AppendEntrys(args *RequestAppendEntrysArgs, reply *RequestAppendEntrysReply) {
	fmt.Println("AppendEntrys")
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	//收到leader消息，更新election定时器
	if rf.role == roleCandidate {
		rf.becomeFollower()
	} else {
		rf.resetElectionTimer()
	}

	//尚未定时发送heartbeat,所以先关掉ElectionTimer
	rf.stopElectionTimer()

	if nil != args.Entries && len(args.Entries) > 0 {
		entry := rf.getEntryByIndex(args.PrevLogIndex)
		if nil == entry || entry.Term != args.PrevLogTerm {
			//没有通过一致性检查
			if nil == entry {
				reply.Term = 0
			} else {
				reply.Term = entry.Term
			}
			reply.Success = false
			return
		}

		//将entry添加到本地log
		for _, v := range args.Entries {
			rf.log = append(rf.log, v)
		}
	}

	rf.leader = args.LeaderId
	rf.commitIndex = args.LeaderCommit
	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntrys(server int, args *RequestAppendEntrysArgs, reply *RequestAppendEntrysReply) bool {
	fmt.Println("sendAppendEntrys")
	ok := rf.peers[server].Call("Raft.AppendEntrys", args, reply)
	return ok
}

func (rf *Raft) getLastEntry() *logEntry {
	l := len(rf.log)
	if l > 1 {
		return &rf.log[l-1]
	} else {
		return nil
	}
}

func (rf *Raft) sendHeartbeat() {
	heartbeat := &RequestAppendEntrysArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}

	for k, _ := range rf.peers {
		if k != rf.me {
			//heartbeat不关心reply
			go rf.sendAppendEntrys(k, heartbeat, &RequestAppendEntrysReply{})
		}
	}
}

func (rf *Raft) vote(server int, term int, lastEntry *logEntry) {
	request := &RequestVoteArgs{
		Term:        term,
		CandidateId: rf.me,
	}

	if nil != lastEntry {
		request.LastLogIndex = lastEntry.Index
		request.LastLogTerm = lastEntry.Term
	}

	reply := &RequestVoteReply{}

	rf.sendRequestVote(server, request, reply)

	if reply.VoteGranted {
		fmt.Println("got vote from", server, term)
		rf.mu.Lock()
		rf.voteNum++
		if rf.voteNum > len(rf.peers)/2 {
			//获得多数集的支持
			rf.becomeLeader()
		}
		rf.mu.Unlock()
	} else {
		rf.mu.Lock()
		if reply.Term >= rf.currentTerm {
			rf.currentTerm = reply.Term
		}
		rf.mu.Unlock()
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	fmt.Println("start", rf.me)

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) becomeFollower() {
	rf.votedFor = nil
	rf.role = roleFollower
	rf.resetElectionTimer()
}

func (rf *Raft) becomeCandidate() {
	var term int
	var lastEntry *logEntry
	rf.mu.Lock()
	rf.votedFor = nil
	rf.role = roleCandidate
	rf.currentTerm++
	rf.voteNum = 1 //先给自己投一票
	term = rf.currentTerm
	lastEntry = rf.getLastEntry()
	rf.mu.Unlock()
	rf.resetElectionTimer()
	//向除自己以外的peer发送vote
	for k, _ := range rf.peers {
		if k != rf.me {
			go rf.vote(k, term, lastEntry)
		}
	}
}

func (rf *Raft) becomeLeader() {
	if rf.stopElectionTimer() {

		fmt.Println("becomeLeader", rf.me)
		rf.role = roleLeader
		rf.leader = rf.me
		nextIndex := 1
		lastEntry := rf.getLastEntry()
		if nil != lastEntry {
			nextIndex = lastEntry.Index
		}

		for k, v := range rf.peers {
			if k != rf.me {
				v.matchIndex = 0
				v.nextIndex = nextIndex
			}
		}

		go rf.sendHeartbeat()
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	//rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.leader = -1
	rf.log = append([]logEntry{}, logEntry{})
	rf.peers = make([]*peer, len(peers), len(peers))

	for k, v := range peers {
		rf.peers[k] = &peer{
			nextIndex:       1,
			matchIndex:      0,
			end:             v,
			lastCommunicate: time.Now(),
		}
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.becomeFollower()

	return rf
}
