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

const (
	FOLLOWER  = "FOLLOWER"
	LEADER    = "LEADER"
	CANDIDATE = "CANDIDATE"
)

var (
	LOG = true
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
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// TODO:Your data here (2A, 2B, 2C).

	currentIdentity string
	currentLeaderId int

	currentTerm int
	votedFor    int

	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.logState("term:%d, leader %t \n", rf.currentTerm, rf.currentIdentity == LEADER)
	return rf.currentTerm, rf.currentIdentity == LEADER
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

func (rf *Raft) logState(format string, a ...interface{}) {
	//fmt.Printf("[id:%d-%s] term:%d votedFor:%d leader:%d |",
	//	rf.me, rf.currentIdentity, rf.currentTerm, rf.votedFor, rf.currentLeaderId)
	//fmt.Printf(format+"\n", a...)
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

func (rf *Raft) tryTransitionToCandidate() {
	if rf.currentIdentity == LEADER {
		return
	}
	rf.currentLeaderId = -1
	rf.currentIdentity = CANDIDATE
	rf.currentTerm++
	// generate a random expireAt, then try transition to Follower
	go func() {
		time.Sleep(time.Duration(randRange(400, 750)))
		//when the term is timeout
		//but it still is a candidate
		//then try transition to candidate again
		if rf.currentIdentity == CANDIDATE {
			rf.logState("[tryTTC] election timeout")
			rf.tryTransitionToCandidate()
			rf.logState("[tryTTC] after TT Candidate")
		}
	}()
	// waiting the vote result
	if rf.StartRequestVote() {
		// win
		rf.logState("[tryTTC] win election")
		rf.tryTransitionToLeader()
		rf.logState("[tryTTC] after TT Leader")
	}

}

func (rf *Raft) tryTransitionToFollower(leaderId int) {
	if rf.currentIdentity == FOLLOWER {
		return
	}
	rf.votedFor = -1
	rf.currentIdentity = FOLLOWER
	rf.currentLeaderId = leaderId
}

func (rf *Raft) tryTransitionToLeader() {
	if rf.currentIdentity != CANDIDATE {
		return
	}
	rf.currentIdentity = LEADER
	rf.currentLeaderId = rf.me
	go rf.StartAppendEntries()
}

func (rf *Raft) StartAppendEntries() {
	//var wg = sync.WaitGroup{}
	// TODO:for loop,sleep
	for {
		if rf.currentIdentity == LEADER {
			rf.logState("[StartAppendEntries] start HB")
			for i := range rf.peers {
				go func(server int) {
					//defer wg.Done()
					var result = AppendEntriesReply{}
					rf.sendAppendEntries(server, &AppendEntriesArgs{
						Term:     rf.currentTerm,
						LeaderId: rf.me,
					}, &result)

				}(i)
			}
		} else {
			rf.logState("[StartAppendEntries] break")

			break
		}
		time.Sleep(time.Duration(randRange(50, 100)))
	}

}

// Start the vote, if win the vote,return true
func (rf *Raft) StartRequestVote() bool {
	var currentTerm = -1
	// vote to it self
	rf.votedFor = rf.me
	var wg = sync.WaitGroup{}
	var count = 0
	var mu sync.Mutex
	//send to each rf.peers
	for i := range rf.peers {
		wg.Add(1)
		//TODO: Is this necessary?
		go func(server int, count *int) {
			defer wg.Done()
			var result = RequestVoteReply{}
			rf.sendRequestVote(server, &RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateId: rf.me,
			}, &result)
			if result.Term > rf.currentTerm {
				currentTerm = result.Term
			}
			rf.logState("[StartRequestVote] voted result:%t,term:%d", result.VoteGranted, result.Term)

			if result.VoteGranted {
				mu.Lock()
				*count++
				mu.Unlock()
			}
		}(i, &count)
	}
	wg.Wait()
	var maj = len(rf.peers)/2 + 1

	rf.logState("[StartRequestVote] received Term:%d,count: %d", currentTerm, count)
	// while rf's current term is expired
	if currentTerm > rf.currentTerm {
		rf.currentTerm = currentTerm
		rf.logState("[StartRequestVote] TT Follower")
		rf.tryTransitionToFollower(-1)
		rf.logState("[StartRequestVote] after TT Follower")
		return false
	}
	if count >= maj {
		rf.logState("[StartRequestVote] win election")
		return true
	}
	return false
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	//TODO: Your data here (2B).
	Term        int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	var voteGranted = false
	if rf.currentTerm > args.Term {
		voteGranted = false
	}
	//TODO: Your code here (2B).
	if rf.votedFor == args.CandidateId || rf.votedFor == -1 {
		voteGranted = true
		rf.votedFor = args.CandidateId
	}
	rf.logState("[RequestVote] voteGranted: %t", voteGranted)

	reply.Term = rf.currentTerm
	reply.VoteGranted = voteGranted

	// discover higher term
	if rf.currentIdentity == CANDIDATE && args.Term > rf.currentTerm {
		rf.tryTransitionToFollower(-1)
		rf.logState("[RequestVote] after Candidate TTC Follower: %t", voteGranted)
	}
	// discover higher term
	if rf.currentIdentity == LEADER && args.Term > rf.currentTerm {
		rf.tryTransitionToFollower(-1)
		rf.logState("[RequestVote] after Leader TTC Follower: %t", voteGranted)
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
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//handler
func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	var success bool

	if rf.currentTerm > args.Term {
		//invalid
		success = false
		rf.logState("[RequestAppendEntries] false")

	} else {
		// discover new term
		if rf.currentIdentity == CANDIDATE && args.LeaderId != rf.me {
			rf.tryTransitionToFollower(args.LeaderId)
			rf.logState("[RequestAppendEntries] after Candidate TT follower")
		}
		// discover higher term
		if rf.currentIdentity == LEADER && args.Term > rf.currentTerm {
			rf.tryTransitionToFollower(args.LeaderId)
			rf.logState("[RequestAppendEntries] after Leader TT follower")
		}

		rf.currentLeaderId = args.LeaderId
		rf.currentTerm = args.Term
		success = true
		rf.logState("[RequestAppendEntries] after update")
	}

	reply.Term = rf.currentTerm
	reply.Success = success

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
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

	// Your initialization code here (2A, 2B, 2C).
	// TODO: 2A

	// init state
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.currentLeaderId = -1
	rf.currentIdentity = FOLLOWER
	// a routing to generate random expired at
	// waiting for leader's AP
	//TODO: generate random time out
	//if leaderId= -1, to candidate
	rf.logState("init")

	go func() {
		time.Sleep(time.Duration(randRange(0, 700)))
		for {
			if rf.currentLeaderId == -1 && rf.currentIdentity == FOLLOWER {
				rf.logState("[Make] timeout")
				rf.tryTransitionToCandidate()
				rf.logState("[Make] after TTC")
			}
			time.Sleep(time.Duration(randRange(750, 1500)))
		}
	}()
	go func() {
		for {
			time.Sleep(time.Duration(randRange(600, 900)))
			if rf.currentIdentity == FOLLOWER {
				rf.currentLeaderId = -1
				rf.votedFor = -1
				rf.logState("[Make] reset")
			}
		}

	}()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func randRange(min, max int) int {
	return (rand.Intn(max-min) + min) * 1000000

}
