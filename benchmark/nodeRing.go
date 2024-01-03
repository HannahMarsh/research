package main

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
)

type NodeRing struct {
	Ring         map[int]int
	SortedHashes []int
	actualNodes  int
	virtualNodes int
}

func NewNodeRing(actualNodes int, virtualNodes int) *NodeRing {
	ring := make(map[int]int)
	var hashes []int

	for i := 0; i < actualNodes; i++ {
		for j := 0; j < virtualNodes; j++ {
			node := fmt.Sprintf("%d", i)
			hash := hashFunc(node+strconv.Itoa(j), virtualNodes)
			ring[hash] = i
			hashes = append(hashes, hash)
		}
	}
	sort.Ints(hashes)
	return &NodeRing{Ring: ring, SortedHashes: hashes, actualNodes: actualNodes, virtualNodes: virtualNodes}
}

func hashFunc(key string, numNodes int) int {
	// Using FNV-1a hash function
	h := fnv.New32a()
	_, err := h.Write([]byte(key))
	if err != nil {
		panic(err)
	}
	return int(h.Sum32())

	// simple hash function to distribute requests across cache nodes
	//var hash int
	//for i := 0; i < len(key); i++ {
	//	hash = (hash*31 + int(key[i])) % numNodes
	//}
	//return hash

}

func (nr *NodeRing) GetNode(key string) int {
	hash := hashFunc(key, nr.virtualNodes)
	idx := sort.Search(len(nr.SortedHashes), func(i int) bool {
		return nr.SortedHashes[i] >= hash
	})
	if idx == len(nr.SortedHashes) {
		idx = 0
	}
	return nr.Ring[nr.SortedHashes[idx]]
}
