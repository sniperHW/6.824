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
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"sync"
	"time"
)

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
	electionTimeoutMin = 500 //1000
	electionTimeoutMax = 800 //1500
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
	id              int
	mu              sync.RWMutex
	nextIndex       int
	matchIndex      int
	end             *labrpc.ClientEnd
	lastCommunicate time.Time
	match           bool //match设置成true前不会发送entry
}

func (p *peer) call(svcMeth string, args interface{}, reply interface{}) bool {
	p.mu.Lock()
	p.lastCommunicate = time.Now()
	p.mu.Unlock()

	return p.end.Call(svcMeth, args, reply)
}

func (p *peer) heartbeatTimeout(now time.Time) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return now.After(p.lastCommunicate.Add(time.Duration(heartbeatInterval) * time.Millisecond))
}

type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	peers     []*peer    // RPC end points of all peers
	persister *Persister // Object to hold this peer's persisted state
	me        int        // this peer's index into peers[]
	leader    int

	//Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []logEntry

	//Volatile state on all servers:
	commitIndex int
	lastApplied int

	voteFrom map[int]bool

	role int

	stopCh  chan struct{}
	doneCh  chan struct{}
	mainCh  chan interface{}
	applyCh chan ApplyMsg

	electionTimeout time.Time

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

func (rf *Raft) apply(entry *logEntry) {
	applyMsg := ApplyMsg{
		CommandValid: true,
		Command:      entry.Command,
		CommandIndex: entry.Index,
	}
	//fmt.Println("server:", rf.me, "apply", *entry)
	rf.applyCh <- applyMsg
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

	return rf.currentTerm, rf.role == roleLeader //rf.leader == rf.me
}

func (rf *Raft) updateTerm(term int) {
	rf.currentTerm = term
}

func (rf *Raft) updateRole(role int) {
	//fmt.Println(rf.me, "updateRole", role)
	rf.role = role
}

func (rf *Raft) updateLeader(leader int) {
	rf.leader = leader
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

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	rf.persister.SaveRaftState(w.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		fmt.Println(rf.me, "readPersist with no state")
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []logEntry

	if err := d.Decode(&currentTerm); err != nil {
		fmt.Printf("[%d] readPersist currentTerm failed of:%s", rf.me, err.Error())
		return
	}
	if err := d.Decode(&votedFor); err != nil {
		fmt.Printf("[%d] readPersist votedFor failed of:%s", rf.me, err.Error())
		return
	}
	if err := d.Decode(&log); err != nil {
		fmt.Printf("[%d] readPersist log failed of:%s", rf.me, err.Error())
		return
	}

	rf.currentTerm = currentTerm
	rf.log = log

	//fmt.Println(rf.me, "read persister", rf.log)
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role == roleLeader {
		if args.Term > rf.currentTerm {
			//leader发现更大的term,becomeFollower
			rf.becomeFollower(args.Term)
		} else {
			//fmt.Println(rf.me, "un vote1 to", args.CandidateId)
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}
	} else {

		if args.Term < rf.currentTerm {
			//收到更小的term,拒绝同时把自身term返回
			reply.Term = rf.currentTerm
			//fmt.Println(rf.me, "un vote1 to", args.CandidateId)
			reply.VoteGranted = false
			return
		} else if args.Term > rf.currentTerm {
			rf.becomeFollower(args.Term)
		} else if rf.votedFor != args.CandidateId {
			//已经投过票了
			//fmt.Println(rf.me, "un vote3 to", args.CandidateId)
			reply.VoteGranted = false
			reply.Term = rf.currentTerm
			return
		}

		lastLogTerm := 0
		lastLogIndex := 0
		lastEntry := rf.getLastEntry()
		if nil != lastEntry {
			lastLogIndex = lastEntry.Index
			lastLogTerm = lastEntry.Term
		}

		if lastLogTerm == args.LastLogTerm {
			if lastLogIndex > args.LastLogIndex {
				reply.Term = rf.currentTerm
				//fmt.Println(rf.me, "un vote4 to", args.CandidateId)
				reply.VoteGranted = false
				return
			}
		} else if lastLogTerm > args.LastLogTerm {
			reply.Term = rf.currentTerm
			//fmt.Println(rf.me, "un vote5 to", args.CandidateId)
			reply.VoteGranted = false
			return
		}

		//可以支持
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	//return rf.peers[server].call("Raft.RequestVote", args, reply)
	p := rf.peers[server]
	go func() {
		ok := p.call("Raft.RequestVote", args, reply)
		//fmt.Println("send vote")
		rf.onRequestVoteReply(p, ok, args, reply)
	}()
}

func (rf *Raft) onRequestVoteReply(p *peer, ok bool, args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.VoteGranted {
			if rf.role == roleCandidate {
				//fmt.Println(rf.me, "got vote from", p.id, "at term", rf.currentTerm)
				rf.voteFrom[p.id] = true
				if len(rf.voteFrom) > len(rf.peers)/2 {
					//获得多数集的支持
					rf.becomeLeader()
				}
			}
		} else {
			if reply.Term >= rf.currentTerm {
				rf.currentTerm = reply.Term
				if rf.role == roleLeader {
					rf.becomeFollower(reply.Term)
				}
			}
		}
	}
}

func (rf *Raft) getEntryByIndex(index int) *logEntry {
	if index < len(rf.log) {
		return &rf.log[index]
	} else {
		return nil
	}
}

func (rf *Raft) AppendEntrys(args *RequestAppendEntrysArgs, reply *RequestAppendEntrysReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		//任何情况下发现更大的term,立刻更新term,并转换成follower
		rf.becomeFollower(args.Term)
	} else {
		if rf.role == roleLeader {
			//fmt.Println(rf.me, "reject 2")
			reply.Success = false
			reply.Term = rf.currentTerm
			return
		} else if rf.role == roleCandidate {
			rf.becomeFollower(args.Term)
		} else {
			rf.resetElectionTimeout()
		}
	}

	preEntry := rf.getEntryByIndex(args.PrevLogIndex)

	if nil == preEntry || preEntry.Term != args.PrevLogTerm {
		//没有通过一致性检查
		if nil == preEntry {
			preEntry = &rf.log[len(rf.log)-1]
		}

		reply.Term = preEntry.Term

		//fmt.Println("server:", rf.me, "reject3 from", args.LeaderId, "because of consistent check", preEntry, args.PrevLogIndex, args.PrevLogTerm)
		reply.Success = false
		return
	}

	lastEntry := rf.getLastEntry()

	if nil != args.Entries && len(args.Entries) > 0 {

		//将args.PrevLogIndex之后的日志删除
		if nil != lastEntry && lastEntry.Index > args.PrevLogIndex {
			//fmt.Println("server:", rf.me, "drop entrys from", args.LeaderId, len(rf.log), args.PrevLogIndex+1, args.Entries, "drops", rf.log[args.PrevLogIndex+1:], time.Now())
			rf.log = rf.log[0 : args.PrevLogIndex+1]
		}

		//将entry添加到本地log
		for _, v := range args.Entries {
			rf.log = append(rf.log, v)
		}

		lastEntry = rf.getLastEntry()
		//oldApply := rf.lastApplied
		if lastEntry.Index >= args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastEntry.Index
		}

		rf.doApply()
		//fmt.Println("server:", rf.me, oldApply, rf.lastApplied, "term", rf.currentTerm, "follower commit", args.LeaderCommit, rf.log)

		rf.persist()

		//fmt.Println("server:", rf.me, "replicate entrys from", args.LeaderId, rf.log, "Entries", args.Entries)
	} else {
		if nil != preEntry && args.LeaderCommit >= preEntry.Index {
			//oldApply := rf.lastApplied
			rf.commitIndex = preEntry.Index
			rf.doApply()
			//fmt.Println("server:", rf.me, oldApply, rf.lastApplied, "term", rf.currentTerm, "follower commit", args.LeaderCommit, rf.log)
		}
	}

	rf.updateLeader(args.LeaderId)
	reply.Success = true
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendAppendEntrys(server int, args *RequestAppendEntrysArgs, reply *RequestAppendEntrysReply) {
	p := rf.peers[server]
	go func() {
		ok := p.call("Raft.AppendEntrys", args, reply)
		rf.onAppendEntrysReply(p, ok, args, reply)
	}()
}

func (rf *Raft) doApply() {

	//oldApply := rf.lastApplied

	for rf.lastApplied < rf.commitIndex && rf.lastApplied+1 < len(rf.log) {
		index := rf.lastApplied + 1
		if index >= len(rf.log) {
			fmt.Println(rf.lastApplied, rf.commitIndex, len(rf.log))
			panic("apply error")
		}
		entry := &rf.log[index]
		rf.apply(entry)
		rf.lastApplied++
	}

	/*if oldApply != rf.lastApplied {
		fmt.Println("server:", rf.me, "apply from", oldApply, "to", rf.lastApplied)
	}*/
}

func (rf *Raft) updateCommited(index int) {
	if index != rf.commitIndex {
		agreeCount := 1
		for _, v := range rf.peers {
			if v.matchIndex == index {
				agreeCount++
			}
		}

		if agreeCount > len(rf.peers)/2 {
			rf.commitIndex = index
			rf.doApply()
		}
	}
}

func (rf *Raft) onAppendEntrysReply(p *peer, ok bool, args *RequestAppendEntrysArgs, reply *RequestAppendEntrysReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if ok {
		if reply.Term > rf.currentTerm {
			//found large term,become follower
			rf.becomeFollower(reply.Term)
		} else {
			if rf.role == roleLeader {
				if reply.Success {
					p.match = true
					//复制成功，更新nextindex和matchindex
					if nil != args.Entries && len(args.Entries) > 0 {
						lastEntry := args.Entries[len(args.Entries)-1]

						p.nextIndex = lastEntry.Index + 1
						p.matchIndex = lastEntry.Index

						if lastEntry.Term == rf.currentTerm {
							//oldCommitIndex := rf.commitIndex
							//oldApply := rf.lastApplied
							rf.updateCommited(lastEntry.Index)
							//if oldCommitIndex != rf.commitIndex {
							//	fmt.Println("server:", rf.me, oldApply, rf.lastApplied, "commitIndex", rf.commitIndex, "leader commit ", p.id, args.Entries)
							//}
						}
					}

					if p.nextIndex != len(rf.log) {
						request := rf.prepareAppendEntriesRequest(p)
						reply := &RequestAppendEntrysReply{}
						rf.sendAppendEntrys(p.id, request, reply)
					}
				} else {

					//fmt.Println("server:", rf.me, "onAppendEntrysReply failed from", p.id, reply.Term, p.nextIndex)

					//follower与leader不匹配，需要调整nextIndex重试
					p.nextIndex = 1

					//需要优化
					if reply.Term != 0 {
						for _, v := range rf.log {
							if v.Term == reply.Term && v.Index > p.matchIndex {
								p.nextIndex = v.Index + 1
								break
							}
						}
					}

					request := rf.prepareAppendEntriesRequest(p)
					reply := &RequestAppendEntrysReply{}
					rf.sendAppendEntrys(p.id, request, reply)
				}
			}
		}
	}
}

func (rf *Raft) getLastEntry() *logEntry {
	l := len(rf.log)
	if l > 1 {
		return &rf.log[l-1]
	} else {
		return nil
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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != roleLeader {
		return index, term, false
	}

	index = 1

	lastEntry := rf.getLastEntry()

	if nil != lastEntry {
		index = lastEntry.Index + 1
	}

	term = rf.currentTerm

	entry := logEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}

	//fmt.Println(rf.me, "AppendEntrys", entry)

	rf.log = append(rf.log, entry)

	rf.persist()

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
	select {
	case rf.stopCh <- struct{}{}:
	default:
	}
	<-rf.doneCh
}

func (rf *Raft) becomeFollower(term int) {

	fmt.Println("server:", rf.me, "becomeFollower at term", term)

	rf.resetElectionTimeout()
	rf.updateRole(roleFollower)
	rf.updateTerm(term)
	rf.leader = -1
	rf.votedFor = -1
	rf.voteFrom = make(map[int]bool)
	rf.persist()
}

func (rf *Raft) becomeCandidate() {

	rf.resetElectionTimeout()
	rf.updateRole(roleCandidate)
	rf.leader = -1
	rf.currentTerm++

	fmt.Println("server:", rf.me, "becomeCandidate at term", rf.currentTerm, time.Now())

	//给自己投票
	rf.votedFor = rf.me
	rf.voteFrom[rf.me] = true
	rf.persist()

	lastEntry := rf.getLastEntry()

	request := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}

	if nil != lastEntry {
		request.LastLogIndex = lastEntry.Index
		request.LastLogTerm = lastEntry.Term
	}

	for k, _ := range rf.peers {
		if k != rf.me {
			rf.sendRequestVote(k, request, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) becomeLeader() {

	fmt.Println("server:", rf.me, "becomeLeader at term", rf.currentTerm)

	rf.clearElectionTimeout()
	rf.updateRole(roleLeader)
	rf.leader = rf.me

	//begin log replicate

	//第一个请求不带entrise,检查follower的日志与leader是否一致
	request := &RequestAppendEntrysArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}

	nextIndex := 1
	lastEntry := rf.getLastEntry()
	if nil != lastEntry {
		nextIndex = lastEntry.Index
		request.PrevLogIndex = lastEntry.Index
		request.PrevLogTerm = lastEntry.Term
	}

	for _, v := range rf.peers {
		if v.id != rf.me {
			v.nextIndex = nextIndex
			v.matchIndex = 0
			v.match = false
			reply := &RequestAppendEntrysReply{}
			rf.sendAppendEntrys(v.id, request, reply)
		}
	}
}

func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = time.Now().Add(time.Duration(randBetween(electionTimeoutMin, electionTimeoutMax)) * time.Millisecond)
}

func (rf *Raft) clearElectionTimeout() {
	rf.electionTimeout = time.Time{}
}

func (rf *Raft) prepareAppendEntriesRequest(p *peer) *RequestAppendEntrysArgs {
	if p.nextIndex >= len(rf.log) {
		//fmt.Println("send heartbeat only", p.id, len(rf.log), p.matchIndex, p.nextIndex)
		//没有entry需要复制，发送心跳

		request := &RequestAppendEntrysArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}

		lastEntry := rf.getLastEntry()
		if nil != lastEntry {
			request.PrevLogIndex = lastEntry.Index
			request.PrevLogTerm = lastEntry.Term
		}

		return request

	} else {

		//fmt.Println(rf.me, p.matchIndex, p.nextIndex)

		preEntry := rf.log[p.nextIndex-1]

		if preEntry.Index != p.nextIndex-1 {
			panic("preEntry.Index != p.nextIndex-1")
		}

		request := &RequestAppendEntrysArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: preEntry.Index,
			PrevLogTerm:  preEntry.Term,
		}

		if p.match {
			//如果只是检测日志是否匹配，不附带entry
			count := len(rf.log) - p.nextIndex
			request.Entries = make([]logEntry, 0, count)
			for i := p.nextIndex; i < len(rf.log); i++ {
				request.Entries = append(request.Entries, rf.log[i])
			}
			//fmt.Println(rf.me, "prepareAppendEntriesRequest to", p.id, request.Entries, p.nextIndex, len(rf.log))
		}
		return request
	}
}

func (rf *Raft) tick() {

	now := time.Now()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !rf.electionTimeout.IsZero() && now.After(rf.electionTimeout) {
		if rf.role == roleCandidate {
			rf.becomeFollower(rf.currentTerm)
			return
		} else if rf.role == roleFollower {
			rf.becomeCandidate()
			return
		}
	}

	if rf.role == roleLeader {
		for _, v := range rf.peers {
			if v.id != rf.me && v.heartbeatTimeout(now) {
				request := rf.prepareAppendEntriesRequest(v)
				reply := &RequestAppendEntrysReply{}
				rf.sendAppendEntrys(v.id, request, reply)
			}
		}
	}
}

func (rf *Raft) mainRoutine() {

	//fmt.Println(rf.me, "mainRoutine start")

	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.becomeFollower(rf.currentTerm)
	}()

	tickChan := make(chan struct{}, 1)
	ticker := time.AfterFunc(time.Millisecond*10, func() {
		select {
		case tickChan <- struct{}{}:
		default:
		}
	})

	//ticker := time.NewTimer(time.Millisecond * 10)
	defer ticker.Stop()

	for {
		select {
		case e := <-rf.mainCh:
			e.(func())()
		case <-rf.stopCh:
			goto EndMainRountine
		case <-tickChan:
			rf.tick()
			ticker.Reset(time.Millisecond * 10)
		}
	}

EndMainRountine:
	//fmt.Println(rf.me, "mainRoutine stop")
	rf.doneCh <- struct{}{}
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
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.leader = -1
	rf.log = append([]logEntry{}, logEntry{})
	rf.peers = make([]*peer, len(peers), len(peers))
	rf.voteFrom = make(map[int]bool)
	rf.stopCh = make(chan struct{}, 1)
	rf.mainCh = make(chan interface{})
	rf.doneCh = make(chan struct{})
	rf.applyCh = applyCh

	for k, v := range peers {
		rf.peers[k] = &peer{
			id:              k,
			nextIndex:       1,
			matchIndex:      0,
			end:             v,
			lastCommunicate: time.Now(),
		}
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.mainRoutine()

	return rf
}
