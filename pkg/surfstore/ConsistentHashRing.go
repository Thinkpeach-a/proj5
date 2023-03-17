package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	//blockHash := c.Hash(blockId)
	var hashKeys []string
	for hash, _ := range c.ServerMap {
		hashKeys = append(hashKeys, hash)
	}
	sort.Strings(hashKeys)
	for _, dat := range hashKeys {
		if blockId <= dat {
			return c.ServerMap[dat]
		}
	}
	return c.ServerMap[hashKeys[0]]
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	c := &ConsistentHashRing{}
	c.ServerMap = make(map[string]string)
	for _, serverAddr := range serverAddrs {
		blockServerAddr := "blockstore" + serverAddr
		serverHash := c.Hash(blockServerAddr)
		c.ServerMap[serverHash] = serverAddr
	}
	return c
}
