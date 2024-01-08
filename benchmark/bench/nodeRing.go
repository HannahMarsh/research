package bench

import (
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
)

// NodeRing stores a map and sorted list of hashes
type NodeRing struct {
	Ring         map[int]int // maps hash to node index
	SortedHashes []int       // sorted list of hashes
	actualNodes  int         // # of actual nodes
	virtualNodes int         // # of virtual nodes per actual node
}

// NewNodeRing creates a new NodeRing with the specified number of actual and virtual nodes.
func NewNodeRing(actualNodes int, virtualNodes int) *NodeRing {
	ring := make(map[int]int) // map to store the ring
	var hashes []int          // slice to store the sorted hashes

	// populate the ring with virtual nodes
	for i := 0; i < actualNodes; i++ {
		for j := 0; j < virtualNodes; j++ {
			node := fmt.Sprintf("%d", i)             // convert node index to string
			hash := hashFunc(node + strconv.Itoa(j)) // hash the node id
			ring[hash] = i                           // map hash to actual node index
			hashes = append(hashes, hash)            // keep track of the hash
		}
	}
	sort.Ints(hashes) // sort the hashes for binary search
	return &NodeRing{
		Ring:         ring,
		SortedHashes: hashes,
		actualNodes:  actualNodes,
		virtualNodes: virtualNodes,
	}
}

// hashFunc computes a hash for the key using FNV-1a algorithm.
func hashFunc(key string) int {
	h := fnv.New32a()              // create 32-bit FNV-1a hash
	_, err := h.Write([]byte(key)) // write to the hash
	if err != nil {
		panic(err)
	}
	return int(h.Sum32())

	// old
	// var hash int
	// for i := 0; i < len(key); i++ {
	//	 hash = (hash*31 + int(key[i])) % numNodes
	// }
	// return hash
}

// GetNode returns the node index a key maps to.
func (nr *NodeRing) GetNode(key string) int {
	hash := hashFunc(key) // first, get the hash for the key

	// binary search: find the index of the first hash that is >= to the key's hash
	idx := sort.Search(len(nr.SortedHashes), func(i int) bool {
		return nr.SortedHashes[i] >= hash
	})
	if idx == len(nr.SortedHashes) { // may not need this
		idx = 0 // wrap around to the beginning
	}
	return nr.Ring[nr.SortedHashes[idx]]
}
