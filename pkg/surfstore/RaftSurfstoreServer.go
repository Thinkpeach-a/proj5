package surfstore

import (
	context "context"
	//"log"
	//"fmt"
	"math"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	isLeader       bool
	isLeaderMutex  *sync.RWMutex
	term           int64
	log            []*UpdateOperation
	commitIndex    int64
	metaStore      *MetaStore
	lastApplied    int64
	ipAddrList     []string
	id             int
	nextIndex      []int64
	MatchedIndex   []int64
	pendingCommits []*chan bool
	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()
	if s.checkValid(ctx, empty) == false {
		return nil, ERR_NOT_LEADER
	}

	return s.metaStore.GetFileInfoMap(ctx, empty)
}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()
	if s.checkValid(ctx, &emptypb.Empty{}) == false {
		return nil, ERR_NOT_LEADER
	}

	return s.metaStore.GetBlockStoreMap(ctx, hashes)
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	blockStoreAddrs := &BlockStoreAddrs{}
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()
	if s.checkValid(ctx, empty) == false {
		return nil, ERR_NOT_LEADER
	}
	blockStoreAddrs, _ = s.metaStore.GetBlockStoreAddrs(ctx, empty)

	return blockStoreAddrs, nil
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	s.isCrashedMutex.RLock()
	if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()
	if s.checkValid(ctx, &emptypb.Empty{}) == false {
		return nil, ERR_NOT_LEADER
	}
	//append the update information to new log
	//send the log to all the followers
	//set the new log to be commited
	newEntry := &UpdateOperation{
		Term:         s.term,
		FileMetaData: filemeta,
	}
	s.log = append(s.log, newEntry)
	//debug
	//fmt.Println("slog: ", len(s.log))
	pending := make(chan bool)
	s.pendingCommits = append(s.pendingCommits, &pending)
	go s.sendToAllFollowersInParallel(ctx)
	success := <-pending
	if success {
		s.lastApplied = s.commitIndex
		return s.metaStore.UpdateFile(ctx, filemeta)
	}
	return nil, ERR_SERVER_CRASHED
}

// send to followers
func (s *RaftSurfstore) sendToAllFollowersInParallel(ctx context.Context) {
	// send entry to all my followers and count the replies
	pendingId := len(s.pendingCommits) - 1
	responses := make(chan bool, len(s.ipAddrList)-1)
	// contact all the follower, send some AppendEntries call
	for curId, _ := range s.ipAddrList {
		if curId == s.id {
			continue
		}

		go s.sendToFollower(ctx, curId, responses)
	}

	totalResponses := 1
	totalAppends := 1

	// wait in loop for responses
	for {
		result := <-responses
		totalResponses++
		if result {
			totalAppends++
		}
		if totalResponses == len(s.ipAddrList) {
			break
		}
	}

	if totalAppends > len(s.ipAddrList)/2 {
		// TODO put on correct channel
		*s.pendingCommits[pendingId] <- true
		// TODO update commit Index correctly
		s.commitIndex = int64(len(s.log) - 1)
	}
}

// get the
func (s *RaftSurfstore) sendToFollower(ctx context.Context, id int, responses chan bool) {
	nextIndex := s.nextIndex[id]

	dummyAppendEntriesInput := AppendEntryInput{
		Term: s.term,
		// TODO put the right values
		//PrevLogTerm:  s.log[nextIndex-1].Term,
		PrevLogIndex: nextIndex - 1,
		Entries:      s.log[nextIndex:],
		LeaderCommit: s.commitIndex,
	}
	prevLog := 0
	if dummyAppendEntriesInput.PrevLogIndex >= 0 {
		prevLog = int(s.log[nextIndex-1].Term)
	}
	dummyAppendEntriesInput.PrevLogTerm = int64(prevLog)
	addr := s.ipAddrList[id]
	// TODO check all errors
	conn, _ := grpc.Dial(addr, grpc.WithInsecure())
	client := NewRaftSurfstoreClient(conn)
	// continue to try to appendEntries until the nextIndex matches
	for {
		//debug
		//fmt.Println("prevIndex: ", dummyAppendEntriesInput.PrevLogIndex)
		//fmt.Println("leadercommit: ", dummyAppendEntriesInput.LeaderCommit)
		output, _ := client.AppendEntries(ctx, &dummyAppendEntriesInput)
		if output == nil {
			continue
		}
		if output.Success == true {
			s.nextIndex[id] = int64(len(s.log))
			break
		} else {
			if s.nextIndex[id] != 0 {
				s.nextIndex[id] -= 1
			}
			curId := s.nextIndex[id] - 1
			dummyAppendEntriesInput.PrevLogIndex = curId
			term := 0
			if curId >= 0 {
				term = int(s.log[curId].Term)
			}
			dummyAppendEntriesInput.PrevLogTerm = int64(term)
		}
	}
	// TODO check output
	responses <- true
}

// check the valid point: the node is the leader and the majority of the nodes are working
func (s *RaftSurfstore) checkValid(ctx context.Context, empty *emptypb.Empty) bool {
	if !s.isLeader {
		return false
	}
	success, _ := s.SendHeartbeat(ctx, empty)
	//debug
	//fmt.Println("checkValid", success.Flag)
	if success.Flag {
		return true
	}
	return false
}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	output := &AppendEntryOutput{
		Success:      false,
		MatchedIndex: -1,
		Term:         s.term,
	}

	s.isCrashedMutex.RLock()
	if s.isCrashed {
		return output, ERR_SERVER_CRASHED
	}
	s.isCrashedMutex.RUnlock()

	if input.Term < s.term {
		output.Success = false
		return output, nil
	}

	if input.PrevLogIndex != -1 {
		lastLog := s.log[input.PrevLogIndex]
		if lastLog.Term != input.PrevLogTerm {
			output.Success = false
			return output, nil
		}
	}
	for i := 0; i < len(input.Entries); i++ {
		curIndex := i + int(input.PrevLogIndex+1)
		inputEntry := input.Entries[i]
		if curIndex >= len(s.log) {
			s.log = append(s.log, inputEntry)
		} else {
			s.log[curIndex] = inputEntry
		}
	}

	if input.LeaderCommit > s.commitIndex {
		s.commitIndex = int64(math.Min(float64(input.LeaderCommit), float64(len(s.log)-1)))

		for s.lastApplied < s.commitIndex {
			s.lastApplied++
			entry := s.log[s.lastApplied]
			s.metaStore.UpdateFile(ctx, entry.FileMetaData)
		}
	}
	// set the term to be equal
	s.term = input.Term
	s.isLeader = false
	output.Success = true
	return output, nil
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}

	s.term++
	s.isLeaderMutex.Lock()
	s.isLeader = true
	for i := 0; i < len(s.ipAddrList); i++ {
		if i == s.id {
			continue
		}
		s.nextIndex[i] = int64(len(s.log))
	}
	s.isLeaderMutex.Unlock()
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.RLock()
	isCrashed := s.isCrashed
	s.isCrashedMutex.RUnlock()
	if isCrashed {
		return &Success{Flag: false}, ERR_SERVER_CRASHED
	}
	length := len(s.log)
	s.isLeaderMutex.RLock()
	isLeader := s.isLeader
	s.isLeaderMutex.RUnlock()
	if !isLeader {
		return &Success{Flag: false}, ERR_NOT_LEADER
	}

	majorityAlive := false
	aliveCount := 1
	for idx, addr := range s.ipAddrList {
		if int64(idx) == int64(s.id) {
			continue
		}

		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			return &Success{Flag: false}, nil
		}
		client := NewRaftSurfstoreClient(conn)

		// TODO create correct AppendEntryInput from s.nextIndex, etc
		var prevLogTerm int64
		if s.commitIndex == -1 {
			prevLogTerm = 0
		} else {
			prevLogTerm = s.log[length-1].Term
		}
		input := &AppendEntryInput{
			Term:         s.term,
			PrevLogTerm:  prevLogTerm,
			PrevLogIndex: int64(length - 1),
			// TODO figure out which entries to send
			Entries:      nil,
			LeaderCommit: s.commitIndex,
		}
		//debug
		//fmt.Println("input***: ", input.Term, input.PrevLogIndex, input.LeaderCommit)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		output, err := client.AppendEntries(ctx, input)
		if output != nil {
			if err == nil {
				aliveCount++
			}
			if aliveCount > len(s.ipAddrList)/2 {
				majorityAlive = true
			}
		} else {
			continue
		}
		if output.Success == true {
			continue
		}
		if output.Term > s.term {
			s.isLeader = false
			break
		}
		//debug
		//fmt.Println("curLog*** id: ", s.id, "leader:", s.isLeader)
		// if the appendentry fail
		for {
			s.nextIndex[idx] -= 1
			curId := s.nextIndex[idx] - 1
			input.PrevLogIndex = curId
			term := 0
			if curId >= 0 {
				term = int(s.log[curId].Term)
			}
			input.PrevLogTerm = int64(term)
			input.Entries = s.log[curId+1:]
			output, _ := client.AppendEntries(ctx, input)
			if output.Success {
				s.nextIndex[idx] = int64(length)
				break
			}
		}
	}
	return &Success{Flag: majorityAlive}, nil
}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()

	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader:    s.isLeader,
		Term:        s.term,
		Log:         s.log,
		MetaMap:     fileInfoMap,
		nextIndex:   s.nextIndex,
		commitIndex: s.commitIndex,
		isCrashed:   s.isCrashed,
	}
	s.isLeaderMutex.RUnlock()
	//fmt.Println(s.isLeader, s.term)
	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)
