package cache

import (
	"encoding/hex"
	"fmt"
	"github.com/spaolacci/murmur3"
	"math/rand"
	"sort"
)

// NodeRing stores a map and sorted list of hashes
type NodeRing struct {
	Ring         map[int]int // maps hash to node index
	SortedHashes []int       // sorted list of hashes
	actualNodes  int         // # of actual nodes
	virtualNodes int         // # of virtual nodes per actual node
}

// generateRandomString creates a random string of a given length.
func generateRandomString(length int) string {
	bytes := make([]byte, length)
	_, err := rand.Read(bytes)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(bytes)
}

// NewNodeRing creates a new NodeRing with the specified number of actual and virtual nodes.
func NewNodeRing(actualNodes int, virtualNodes int) *NodeRing {
	ring := make(map[int]int) // map to store the ring
	var hashes []int          // slice to store the sorted hashes

	// populate the ring with virtual nodes
	for i := 0; i < actualNodes; i++ {
		for j := 0; j < virtualNodes; j++ {
			// Generate a unique identifier for each virtual node
			nodeID := fmt.Sprintf("%s", generateRandomString(10))
			hash := hashFunc(nodeID)      // hash the unique node identifier
			ring[hash] = i                // map hash to actual node index
			hashes = append(hashes, hash) // keep track of the hash
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

// hashFunc computes a hash for the key using MurmurHash algorithm.
func hashFunc(key string) int {
	// Using MurmurHash to compute the hash
	hash := murmur3.New32() // create a new 32-bit MurmurHash3 hash
	_, err := hash.Write([]byte(key))
	if err != nil {
		panic(err)
	}
	return int(hash.Sum32())
}

// GetNode returns the node index a key maps to.
func (nr *NodeRing) GetNode(key string) int {

	//rand.Seed(time.Now().UnixNano())
	//
	//return rand.Intn(nr.actualNodes)

	hash := hashFunc(key) // first, get the hash for the key

	// binary search: find the index of the first hash that is >= to the key's hash
	idx := sort.Search(len(nr.SortedHashes), func(i int) bool {
		return nr.SortedHashes[i] >= hash
	}) % len(nr.SortedHashes)
	if index, exists := nr.Ring[nr.SortedHashes[idx]]; exists {
		return index
	} else {
		panic("NodeRing: GetNode: node does not exist")
	}
}
