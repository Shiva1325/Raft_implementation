package service

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"
)

const DEBUG = 0

type RNState int

const (
	Follower RNState = iota
	Candidate
	Leader
	Dead
)

type CommitEntry struct {
	Command interface{}
	Term    uint64
	Index   uint64
}

type LogEntry struct {
	Command interface{}
	Term    uint64
}

type RaftNode struct {
	id                 uint64
	mu                 sync.Mutex
	peerList           Set
	server             *Server
	db                 *Database
	commitChan         chan CommitEntry
	newCommitReady     chan struct{}
	trigger            chan struct{}
	currentTerm        uint64
	votedFor           int
	log                []LogEntry
	commitIndex        uint64
	lastApplied        uint64
	state              RNState
	electionResetEvent time.Time
	nextIndex          map[uint64]uint64
	matchIndex         map[uint64]uint64
}

type RequestVoteArgs struct {
	Term         uint64
	CandidateId  uint64
	LastLogIndex uint64
	LastLogTerm  uint64
}

type RequestVoteReply struct {
	Term        uint64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term     uint64
	LeaderId uint64

	PrevLogIndex uint64
	PrevLogTerm  uint64
	Entries      []LogEntry
	LeaderCommit uint64
}

type AppendEntriesReply struct {
	Term          uint64
	Success       bool
	ConflictIndex uint64
	ConflictTerm  uint64
}
type AddServers struct {
	ServerIds []int
}

type RemoveServers struct {
	ServerIds []int
}
type Read struct {
	Key string
}

// debug logs a debug message if the debug level is set to DEBUG
func (rn *RaftNode) debug(format string, args ...interface{}) {
	if DEBUG > 0 {
		format = fmt.Sprintf("[%d] %s", rn.id, format)
		log.Printf(format, args...)
	}
}

// NewRaftNode  creates  a  new  Raft  node  with the given id, peers, server, database, and commit channel.
func NewRaftNode(id uint64, peerList Set, server *Server, db *Database, ready <-chan interface{}, commitChan chan CommitEntry) *RaftNode {
	node := &RaftNode{
		id:                 id,
		peerList:           peerList,
		server:             server,
		db:                 db,
		commitChan:         commitChan,
		newCommitReady:     make(chan struct{}, 16),
		trigger:            make(chan struct{}, 1),
		currentTerm:        0,
		votedFor:           -1,
		log:                make([]LogEntry, 0),
		commitIndex:        0,
		lastApplied:        0,
		state:              Follower,
		electionResetEvent: time.Now(),
		nextIndex:          make(map[uint64]uint64),
		matchIndex:         make(map[uint64]uint64),
	}
	if node.db.HasData() {
		node.restoreFromStorage()
	}
	go func() {
		<-ready
		node.mu.Lock()
		node.electionResetEvent = time.Now()
		node.mu.Unlock()
		node.runElectionTimer()
	}()
	go node.sendCommit()
	return node
}

// sendCommit  sends  committed  entries on commit channel.
func (rn *RaftNode) sendCommit() {
	for range rn.newCommitReady {
		rn.mu.Lock()
		savedTerm := rn.currentTerm
		savedLastApplied := rn.lastApplied
		var entries []LogEntry
		if rn.commitIndex > rn.lastApplied {
			entries = rn.log[rn.lastApplied:rn.commitIndex]
			rn.lastApplied = rn.commitIndex
		}
		rn.mu.Unlock()
		rn.debug("sendCommit entries=%v, savedLastApplied=%d", entries, savedLastApplied)
		for i, entry := range entries {
			rn.commitChan <- CommitEntry{
				Command: entry.Command,
				Index:   savedLastApplied + uint64(i) + 1,
				Term:    savedTerm,
			}
		}
	}
	rn.debug("sendCommit completed")
}

func (rn *RaftNode) runElectionTimer() {
	timeoutDuration := rn.electionTimeout()
	rn.mu.Lock()
	termStarted := rn.currentTerm
	rn.mu.Unlock()
	rn.debug("Election Timer started (%v), term=%d", timeoutDuration, termStarted)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		<-ticker.C
		rn.mu.Lock()
		if rn.state != Candidate && rn.state != Follower {
			rn.debug("In election timer state=%s, bailing out", rn.state)
			rn.mu.Unlock()
			return
		}
		if termStarted != rn.currentTerm {
			rn.debug("in election timer term changed from %d to %d, bailing out", termStarted, rn.currentTerm)
			rn.mu.Unlock()
			return
		}
		if elapsed := time.Since(rn.electionResetEvent); elapsed >= timeoutDuration {
			rn.startElection()
			rn.mu.Unlock()
			return
		}
		rn.mu.Unlock()
	}
}

func (rn *RaftNode) electionTimeout() time.Duration {
	if os.Getenv("RAFT_FORCE_MORE_REELECTION") == "true" && rand.Intn(3) > 0 {
		return time.Duration(150) * time.Millisecond
	} else {
		return time.Duration(150+rand.Intn(150)) * time.Millisecond

	}
}

// startElection starts a new  election  with  the current Raft Node as a candidate.
func (rn *RaftNode) startElection() {
	rn.state = Candidate
	rn.currentTerm += 1
	savedCurrentTerm := rn.currentTerm
	rn.electionResetEvent = time.Now()
	rn.votedFor = int(rn.id)
	votesReceived := 1

	rn.debug("Becomes Candidate (currentTerm=%d); log=%v", savedCurrentTerm, rn.log)
	go func() {
		rn.mu.Lock()
		defer rn.mu.Unlock()

		if rn.state != Candidate {
			rn.debug("while waiting for majority, state = %v", rn.state)
			return
		}
		if votesReceived*2 > rn.peerList.Size()+1 {
			rn.debug("Wins election with %d votes", votesReceived)
			rn.becomeLeader()
			return
		}
	}()
	for peer := range rn.peerList.peerSet {
		go func(peer uint64) {
			rn.mu.Lock()
			savedLastLogIndex, savedLastLogTerm := rn.lastLogIndexAndTerm()
			rn.mu.Unlock()

			args := RequestVoteArgs{
				Term:         savedCurrentTerm,
				CandidateId:  rn.id,
				LastLogIndex: savedLastLogIndex,
				LastLogTerm:  savedLastLogTerm,
			}
			rn.debug("Sending RequestVote to %d: %+v", peer, args)
			var reply RequestVoteReply
			if err := rn.server.RPC(peer, "RaftNode.RequestVote", args, &reply); err == nil {
				rn.mu.Lock()
				defer rn.mu.Unlock()
				rn.debug("received RequestVoteReply %+v from %v", reply, peer)

				if rn.state != Candidate {
					rn.debug("While waiting for reply, state = %v", rn.state)
					return
				}

				if reply.Term > savedCurrentTerm {
					rn.debug("Term out of date in RequestVoteReply from %v", peer)
					rn.becomeFollower(reply.Term)
					return
				} else if reply.Term == savedCurrentTerm {
					if reply.VoteGranted {
						votesReceived += 1
						if votesReceived*2 > rn.peerList.Size()+1 {
							rn.debug("Wins election with %d votes", votesReceived)
							log.Printf(" %d Wins election with %d votes", rn.id, votesReceived)
							rn.becomeLeader() // Become leader
							return
						}
					}
				}
			}
		}(peer)
	}
	go rn.runElectionTimer()
}

// becomeLeader switches Raft Node into a leader  state and begins process of heartbeats every 50ms.
func (rn *RaftNode) becomeLeader() {
	rn.state = Leader
	for peer := range rn.peerList.peerSet {
		rn.nextIndex[peer] = uint64(len(rn.log)) + 1
		rn.matchIndex[peer] = 0
	}
	rn.debug("becomes Leader; term=%d, nextIndex=%v, matchIndex=%v; log=%v", rn.currentTerm, rn.nextIndex, rn.matchIndex, rn.log)
	go func(heartbeatTimeout time.Duration) {
		rn.leaderSendAEs()

		t := time.NewTimer(heartbeatTimeout)
		defer t.Stop()
		for {
			doSend := false
			select {
			case <-t.C:
				doSend = true
				t.Stop()
				t.Reset(heartbeatTimeout)

			case _, ok := <-rn.trigger:
				if ok {
					doSend = true
				} else {
					return
				}
				if !t.Stop() {
					<-t.C
				}
				t.Reset(heartbeatTimeout)
			}

			if doSend {
				rn.mu.Lock()
				if rn.state != Leader {
					rn.mu.Unlock()
					return
				}
				rn.mu.Unlock()
				rn.leaderSendAEs()
			}
		}
	}(50 * time.Millisecond)
}

// leaderSendAEs sends AppendEntries RPCs to all peers in the cluster, collects responses, and updates the
// state  of  the Raft Node accordingly.
func (rn *RaftNode) leaderSendAEs() {
	rn.mu.Lock()
	savedCurrentTerm := rn.currentTerm
	rn.mu.Unlock()
	go func(peer uint64) {
		if rn.peerList.Size() == 0 {
			if uint64(len(rn.log)) > rn.commitIndex {
				savedCommitIndex := rn.commitIndex
				for i := rn.commitIndex + 1; i <= uint64(len(rn.log)); i++ {
					if rn.log[i-1].Term == rn.currentTerm {
						rn.commitIndex = i

					}
				}
				if savedCommitIndex != rn.commitIndex {
					rn.debug("Leader sets commitIndex := %d", rn.commitIndex)
					rn.newCommitReady <- struct{}{}
					rn.trigger <- struct{}{}
				}
			}
		}
	}(rn.id)
	for peer := range rn.peerList.peerSet {
		go func(peer uint64) {
			rn.mu.Lock()
			nextIndex := rn.nextIndex[peer]
			prevLogIndex := int(nextIndex) - 1
			prevLogTerm := uint64(0)

			if prevLogIndex > 0 {
				prevLogTerm = rn.log[uint64(prevLogIndex)-1].Term
			}
			entries := rn.log[int(nextIndex)-1:]

			args := AppendEntriesArgs{
				Term:         savedCurrentTerm,
				LeaderId:     rn.id,
				PrevLogIndex: uint64(prevLogIndex),
				PrevLogTerm:  prevLogTerm,
				Entries:      entries,
				LeaderCommit: rn.commitIndex,
			}

			rn.mu.Unlock()
			rn.debug("sending AppendEntries to %v: ni=%d, args=%+v", peer, nextIndex, args)

			var reply AppendEntriesReply
			if err := rn.server.RPC(peer, "RaftNode.AppendEntries", args, &reply); err == nil {
				rn.mu.Lock()
				defer rn.mu.Unlock()
				if reply.Term > savedCurrentTerm {

					rn.debug("Term out of date in heartbeat reply")
					rn.becomeFollower(reply.Term)
					return
				}
				if rn.state == Leader && savedCurrentTerm == reply.Term {
					if reply.Success {
						rn.nextIndex[peer] = nextIndex + uint64(len(entries))
						rn.matchIndex[peer] = rn.nextIndex[peer] - 1

						savedCommitIndex := rn.commitIndex
						for i := rn.commitIndex + 1; i <= uint64(len(rn.log)); i++ {
							if rn.log[i-1].Term == rn.currentTerm {
								matchCount := 1

								for peer := range rn.peerList.peerSet {
									if rn.matchIndex[peer] >= i {
										matchCount++
									}
								}
								if matchCount*2 > rn.peerList.Size()+1 {
									rn.commitIndex = i
								}
							}
						}
						rn.debug("AppendEntries reply from %d success: nextIndex := %v, matchIndex := %v; commitIndex := %d", peer, rn.nextIndex, rn.matchIndex, rn.commitIndex)
						if rn.commitIndex != savedCommitIndex {
							rn.debug("Leader sets commitIndex := %d", rn.commitIndex)
							rn.newCommitReady <- struct{}{}
							rn.trigger <- struct{}{}
						}
					} else {
						if reply.ConflictTerm > 0 {
							lastIndexOfTerm := uint64(0)
							for i := uint64(len(rn.log)); i > 0; i-- {
								if rn.log[i-1].Term == reply.ConflictTerm {
									lastIndexOfTerm = i
									break
								}
							}
							if lastIndexOfTerm > 0 {
								rn.nextIndex[peer] = lastIndexOfTerm + 1
							} else {
								rn.nextIndex[peer] = reply.ConflictIndex
							}
						} else {
							rn.nextIndex[peer] = reply.ConflictIndex
						}
						rn.debug("AppendEntries reply from %d !success: nextIndex := %d", peer, nextIndex-1)
					}
				}
			}
		}(peer)
	}
}

// lastLogIndexAndTerm  returns the index  of the last log and the last log entry's term
func (rn *RaftNode) lastLogIndexAndTerm() (uint64, uint64) {
	if len(rn.log) > 0 {
		lastIndex := uint64(len(rn.log))
		return lastIndex, rn.log[lastIndex-1].Term

	} else {
		return 0, 0
	}
}

// AppendEntries  is  the  RPC  handler  for  AppendEntries RPCs. This function is used to send entries to followers
// This function expects the  Node's  mutex  to  be  locked
func (rn *RaftNode) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.state == Dead || !rn.peerList.Exists(args.LeaderId) {
		return nil //	Return no error
	}
	rn.debug("AppendEntries: %+v", args)

	if args.Term > rn.currentTerm {
		rn.debug("Term out of date in AppendEntries")
		rn.becomeFollower(args.Term)
	}

	reply.Success = false
	if args.Term == rn.currentTerm {
		if rn.state != Follower {
			rn.becomeFollower(args.Term)
		}
		rn.electionResetEvent = time.Now()
		if args.PrevLogIndex == 0 ||
			(args.PrevLogIndex <= uint64(len(rn.log)) && args.PrevLogTerm == rn.log[args.PrevLogIndex-1].Term) {
			reply.Success = true
			logInsertIndex := args.PrevLogIndex + 1
			newEntriesIndex := uint64(1)
			for {
				if logInsertIndex > uint64(len(rn.log)) || newEntriesIndex > uint64(len(args.Entries)) {
					break
				}
				if rn.log[logInsertIndex-1].Term != args.Entries[newEntriesIndex-1].Term {
					break
				}
				logInsertIndex++
				newEntriesIndex++
			}
			if newEntriesIndex <= uint64(len(args.Entries)) {
				rn.debug("Inserting entries %v from index %d", args.Entries[newEntriesIndex-1:], logInsertIndex)
				rn.log = append(rn.log[:logInsertIndex-1], args.Entries[newEntriesIndex-1:]...)
				for _, entry := range args.Entries[newEntriesIndex-1:] {
					cmd := entry.Command
					switch v := cmd.(type) {
					case AddServers:
						for _, peerId := range v.ServerIds {
							if rn.id == uint64(peerId) {
								continue
							}
							rn.peerList.Add(uint64(peerId))
						}
					case RemoveServers:
						for _, peerId := range v.ServerIds {
							rn.peerList.Remove(uint64(peerId))
						}
					}
				}

				rn.debug("Log is now: %v", rn.log)
			}
			if args.LeaderCommit > rn.commitIndex {
				rn.commitIndex = uint64(math.Min(float64(args.LeaderCommit), float64(len(rn.log))))
				rn.debug("Setting commitIndex=%d", rn.commitIndex)
				rn.newCommitReady <- struct{}{}
			}
		} else {
			if args.PrevLogIndex > uint64(len(rn.log)) {
				reply.ConflictIndex = uint64(len(rn.log)) + 1
				reply.ConflictTerm = 0
			} else {
				reply.ConflictTerm = rn.log[args.PrevLogIndex-1].Term
				var cfi uint64
				for cfi = args.PrevLogIndex - 1; cfi > 0; cfi-- {
					if rn.log[cfi-1].Term != reply.ConflictTerm {
						break
					}
				}
				reply.ConflictIndex = cfi + 1
			}
		}
	}

	reply.Term = rn.currentTerm
	rn.persistToStorage()
	rn.debug("AppendEntries reply: %+v", *reply)
	return nil
}

// RequestVote Remote Procedure Call is invoked by candidates to find out if they can win an election.  The RPC  returns
// true if the candidate is running and has a higher term than the current term.
func (rn *RaftNode) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) error {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if rn.state == Dead || !rn.peerList.Exists(args.CandidateId) {
		return nil
	}
	lastLogIndex, lastLogTerm := rn.lastLogIndexAndTerm()
	rn.debug("RequestVote: %+v [currentTerm=%d, votedFor=%d, log index/term=(%d, %d)]", args, rn.currentTerm, rn.votedFor, lastLogIndex, lastLogTerm)
	if args.Term > rn.currentTerm {
		rn.debug("Term out of date with term in RequestVote")
		rn.becomeFollower(args.Term)
	}
	if rn.currentTerm == args.Term &&
		(rn.votedFor == -1 || rn.votedFor == int(args.CandidateId)) &&
		(args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)) {
		reply.VoteGranted = true
		rn.votedFor = int(args.CandidateId)
		rn.electionResetEvent = time.Now()
	} else {
		// Deny the vote to the candidate
		reply.VoteGranted = false
	}
	reply.Term = rn.currentTerm
	rn.persistToStorage()
	rn.debug("RequestVote reply: %+v", reply)
	return nil
}

// becomeFollower makes the current RaftNode  a follower and resets the state.
func (rn *RaftNode) becomeFollower(term uint64) {
	rn.debug("Becomes Follower with term=%d; log=%v", term, rn.log)

	rn.state = Follower
	rn.currentTerm = term
	rn.votedFor = -1
	rn.electionResetEvent = time.Now()

	go rn.runElectionTimer()
}

// persistToStorage saves  all  of  Raft  Node's persistent state in Raft Node's database
func (rn *RaftNode) persistToStorage() {
	for _, data := range []struct {
		name  string
		value interface{}
	}{{"currentTerm", rn.currentTerm}, {"votedFor", rn.votedFor}, {"log", rn.log}} {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		if err := enc.Encode(data.value); err != nil {
			log.Fatal("encode error: ", err)
		}
		rn.db.Set(data.name, buf.Bytes())
	}
}

// restoreFromStorage saves  all  of  Raft  Node's persistent state in Raft Node's database
func (rn *RaftNode) restoreFromStorage() {
	for _, data := range []struct {
		name  string
		value interface{}
	}{{"currentTerm", &rn.currentTerm}, {"votedFor", &rn.votedFor}, {"log", &rn.log}} {
		if value, found := rn.db.Get(data.name); found {
			dec := gob.NewDecoder(bytes.NewBuffer(value))
			if err := dec.Decode(data.value); err != nil {
				log.Fatal("decode error: ", err)
			}
		} else {
			log.Fatal("No data found for", data.name)
		}
	}
}

func (rn *RaftNode) readFromStorage(key string, reply interface{}) error {
	if value, found := rn.db.Get(key); found {
		dec := gob.NewDecoder(bytes.NewBuffer(value))
		if err := dec.Decode(reply); err != nil {
		}
		return nil
	} else {
		err := fmt.Errorf("KeyNotFound:%v", key)
		return err
	}
}

// Submit submits a new command from the client to  the RaftNode.   It  returns true iff this Raft Node is the leader
func (rn *RaftNode) Submit(command interface{}) (bool, interface{}, error) {
	rn.mu.Lock() // Lock the mutex
	rn.debug("Submit received by %v: %v", rn.state, command)
	// Process the command only if the node is a leader
	if rn.state == Leader {
		switch v := command.(type) {
		case Read:
			key := v.Key
			var value int
			readErr := rn.readFromStorage(key, &value)
			rn.mu.Unlock()
			return true, value, readErr
		case AddServers:
			serverIds := v.ServerIds
			for i := 0; i < len(serverIds); i++ {
				if rn.peerList.Exists(uint64(serverIds[i])) {
					rn.mu.Unlock()
					return false, nil, errors.New("server with given serverID already exists")
				}
			}
			rn.log = append(rn.log, LogEntry{Command: command, Term: rn.currentTerm})
			for i := 0; i < len(serverIds); i++ {
				rn.peerList.Add(uint64(serverIds[i]))
				rn.server.peerList.Add(uint64(serverIds[i]))
				rn.nextIndex[uint64(serverIds[i])] = uint64(len(rn.log)) + 1
				rn.matchIndex[uint64(serverIds[i])] = 0
			}
			rn.persistToStorage()
			rn.debug("log=%v", rn.log)
			rn.mu.Unlock()
			rn.trigger <- struct{}{}
			return true, nil, nil
		case RemoveServers:
			serverIds := v.ServerIds
			for i := 0; i < len(serverIds); i++ {
				if !rn.peerList.Exists(uint64(serverIds[i])) && rn.id != uint64(serverIds[i]) {
					rn.mu.Unlock()
					return false, nil, errors.New("server with given serverID does not exist")
				}
			}
			rn.log = append(rn.log, LogEntry{Command: command, Term: rn.currentTerm})
			for i := 0; i < len(serverIds); i++ {
				if rn.id != uint64(serverIds[i]) {
					rn.peerList.Remove(uint64(serverIds[i]))
					rn.server.peerList.Remove(uint64(serverIds[i]))
				}
			}
			rn.persistToStorage()
			rn.debug("log=%v", rn.log)
			rn.mu.Unlock()
			rn.trigger <- struct{}{}
			return true, nil, nil
		default:
			rn.log = append(rn.log, LogEntry{Command: command, Term: rn.currentTerm})
			rn.persistToStorage()
			rn.debug("log=%v", rn.log)
			rn.mu.Unlock()
			rn.trigger <- struct{}{}
			return true, nil, nil
		}
	}
	rn.mu.Unlock() // Unlock the mutex
	return false, nil, nil
}

// Stops the RaftNode, cleaning up  its  state.
func (rn *RaftNode) Stop() {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.state = Dead
	rn.debug("Becomes Dead")
	close(rn.newCommitReady)
}

// Returns the current state of the RaftNode
func (rn *RaftNode) Report() (id int, term int, isLeader bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	isLeader = rn.state == Leader
	return int(rn.id), int(rn.currentTerm), isLeader
}

// Returns the Raft node state.
func (s RNState) String() string {
	switch s {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	case Dead:
		return "Dead"
	default:
		panic("Error: Unknown state")
	}
}
