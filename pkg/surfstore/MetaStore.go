package surfstore

import (
	context "context"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
	"sync"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	mtx                sync.Mutex
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	var curVersion int32
	// if not exist, then set curVersion to zero
	m.mtx.Lock()
	if _, ok := m.FileMetaMap[fileMetaData.Filename]; !ok {
		curVersion = 0
	} else {
		curMetaData := m.FileMetaMap[fileMetaData.Filename]
		curVersion = curMetaData.Version
	}
	updateVersion := fileMetaData.Version
	// do we need to responsible for new files?
	if updateVersion >= curVersion+1 {
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
	} else {
		return &Version{Version: -1}, nil
	}
	m.mtx.Unlock()
	return &Version{Version: updateVersion}, nil
}

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	blockStoreMap := &BlockStoreMap{}
	blockStoreMap.BlockStoreMap = make(map[string]*BlockHashes)
	for _, blockHash := range blockHashesIn.Hashes {
		serverAddr := m.ConsistentHashRing.GetResponsibleServer(blockHash)
		if _, ok := blockStoreMap.BlockStoreMap[serverAddr]; !ok {
			blockHashes := &BlockHashes{}
			blockHashes.Hashes = append(blockHashes.Hashes, blockHash)
			blockStoreMap.BlockStoreMap[serverAddr] = blockHashes
		} else {
			curBlockHashes := blockStoreMap.BlockStoreMap[serverAddr]
			curBlockHashes.Hashes = append(curBlockHashes.Hashes, blockHash)
		}
	}
	return blockStoreMap, nil
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
