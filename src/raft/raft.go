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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

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

const (
	FOLLOWER  = "FOLLOWER"
	CANDIDATE = "CANDIDATE"
	LEADER    = "LEADER"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	state           string
	currentLeaderId int
	currentTerm     int
	lastReceived    time.Time
	votedFor        int
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.log("[GetState] isLeader: %t", rf.currentLeaderId == rf.me)
	return rf.currentTerm, rf.currentLeaderId == rf.me
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
	Term        int
	CandidateId int
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// currentTerm higher than
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else {
		// current smaller or equal
		if rf.votedFor == args.CandidateId || rf.votedFor == -1 {
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			rf.log("[RequestVote] voted to %d", args.CandidateId)
		}
		// current term smaller than args' s
		if rf.currentTerm < args.Term && (rf.state == CANDIDATE || rf.state == LEADER) {
			rf.log("[RequestVote] before TT Follower")
			rf.transitionToFollower(-1, args.Term, time.Time{}, args.CandidateId)
			rf.log("[RequestVote] after TT Follower")
		}
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term   int
	Leader int
}
type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Success = false
	} else {
		if rf.state == CANDIDATE && args.Leader != rf.me {
			rf.transitionToFollower(args.Leader, args.Term, time.Now(), -1)
			rf.log("[AppendEntries] after TT Follower")
		}
		if rf.state == LEADER && args.Term > rf.currentTerm {
			rf.transitionToFollower(args.Leader, args.Term, time.Now(), -1)
			rf.log("[AppendEntries] after TT Leader")
		}
		reply.Term = rf.currentTerm
		reply.Success = true

		rf.currentLeaderId = args.Leader
		rf.currentTerm = args.Term
		rf.lastReceived = time.Now()
		rf.log("[AppendEntries] after update")
	}
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) StartElection() {
	rf.mu.Lock()
	me := rf.me
	term := rf.currentTerm
	rf.mu.Unlock()

	lastTerm := -1
	count := 1
	mu := sync.Mutex{}
	marj := len(rf.peers)/2 + 1
	wg := sync.WaitGroup{}

	go func() {
		time.Sleep(time.Duration(randRange(450, 750)))
		//when the term is timeout
		//but it still is a candidate
		//then try transition to candidate again
		if rf.state == CANDIDATE {
			rf.log("[tryTTC] election timeout")
			rf.transitionToCandidate()
			rf.log("[tryTTC] after TT Candidate")
		}
	}()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		go func(server int, term int, me int) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, &RequestVoteArgs{
				Term:        term,
				CandidateId: me,
			}, reply)

			if !ok {
				wg.Done()
				return
			}

			// discover a higher than itself
			if reply.Term > term {
				mu.Lock()
				lastTerm = reply.Term
				mu.Unlock()
				wg.Done()
				return
			}

			if reply.VoteGranted {
				mu.Lock()
				defer mu.Unlock()
				rf.log("[StartElection] get voted!")
				count++
			}
			wg.Done()
		}(i, term, me)
	}
	wg.Wait()

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastTerm != -1 {
		// discover a higher term
		rf.transitionToFollower(-1, lastTerm, time.Time{}, -1)
		rf.log("[StartElection] after TT Follower")
		return
	}
	rf.log("[StartElection] count: %d", count)
	if count >= marj {
		// win the election
		rf.transitionToLeader()
		rf.log("[StartElection] after TT Leader")
	}
}

func (rf *Raft) StartAppendEntries() {
	for {
		rf.mu.Lock()
		if rf.state == LEADER {
			term := rf.currentTerm
			me := rf.me
			rf.mu.Unlock()
			rf.log("[StartAppendEntries] start HB")
			for i := range rf.peers {
				go func(server, term, me int) {
					var result = AppendEntriesReply{}
					rf.sendAppendEntries(server, &AppendEntriesArgs{
						Term:   term,
						Leader: me,
					}, &result)
					if result.Term > term {
						rf.log("[StartAppendEntries] TT follower")
						rf.transitionToFollower(-1, result.Term, time.Now(), -1)
					}
				}(i, term, me)
			}
		} else {
			rf.mu.Unlock()
			rf.log("[StartAppendEntries] break")
			break
		}

		time.Sleep(time.Duration(randRange(50, 100)))
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.init()
	rf.log("[Make] init")
	// Your initialization code here (2A, 2B, 2C).
	go func() {
		time.Sleep(randRange(450, 750))
		for {
			rf.mu.Lock()
			// didn't received HB from Leader periodically
			if (rf.state == FOLLOWER && rf.lastReceived.IsZero()) ||
				(rf.state == FOLLOWER && time.Now().Sub(rf.lastReceived) > time.Duration(450)*time.Millisecond) {

				rf.log("[MAKE] before reset to Candidate")
				// transition to follower
				rf.transitionToCandidate()
				rf.log("[MAKE] after reset to Candidate")
			}
			rf.mu.Unlock()
			time.Sleep(randRange(600, 900))
		}
	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func randRange(min, max int) time.Duration {
	return time.Duration(rand.Intn(max-min)+min) * time.Millisecond
}

func (rf *Raft) transitionToFollower(leaderId int, term int, lastReceived time.Time, votedFor int) {
	if rf.state == FOLLOWER {
		return
	}
	if term != -1 {
		rf.currentTerm = term
	}
	rf.currentLeaderId = leaderId

	if !lastReceived.IsZero() {
		rf.lastReceived = lastReceived
	}

	rf.state = FOLLOWER

	if votedFor != -1 {
		rf.votedFor = votedFor
	} else {
		votedFor = -1
	}

}

func (rf *Raft) transitionToLeader() {
	if rf.state == FOLLOWER {
		return
	}
	rf.currentLeaderId = rf.me
	rf.state = LEADER
	rf.lastReceived = time.Time{}
	go rf.StartAppendEntries()
}

func (rf *Raft) transitionToCandidate() {
	if rf.state == LEADER {
		return
	}
	rf.currentTerm++
	rf.state = CANDIDATE
	rf.votedFor = rf.me
	rf.lastReceived = time.Time{}
	go rf.StartElection()
}

func (rf *Raft) log(format string, a ...interface{}) {
	//fmt.Printf("\033[1;31;40m [ I:%d | S:%9s | T:%2d | V:%2d | L:%2d | R:%s ] \033[0m ", rf.me, rf.state, rf.currentTerm, rf.votedFor, rf.currentLeaderId, rf.lastReceived)
	//fmt.Printf(format+"\n", a...)
}

func (rf *Raft) init() {
	rf.currentTerm = 0
	rf.currentLeaderId = -1
	rf.lastReceived = time.Time{}
	rf.state = FOLLOWER
	rf.votedFor = -1
}
