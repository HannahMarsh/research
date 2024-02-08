package cache

import (
	"fmt"
	"math"
	"testing"
)

// TestNodeRingDistribution tests that the distribution of keys hashed by `nodeRing` is approximately even across the nodes.
func TestNodeRingDistribution(t *testing.T) {
	// Setup
	actualNodes := 4
	virtualNodes := 50000
	nodeRing := NewNodeRing(actualNodes, virtualNodes)

	// Key generation and distribution tracking
	keyCount := 50000000 // number of keys to test

	digits := int(math.Ceil(math.Log10(float64(keyCount))))
	nodeCounts := make(map[int]int)
	for i := 0; i < keyCount; i++ {
		str := fmt.Sprintf("%%0%dd", digits)
		key := fmt.Sprintf(str, i)
		nodeIndex := nodeRing.GetNode(key)
		nodeCounts[nodeIndex]++
	}

	// Analysis
	expectedPercentage := 1.0 / float64(actualNodes)
	variancePercentage := 0.02 // 1.5% variance allowed

	lower := expectedPercentage - variancePercentage
	upper := expectedPercentage + variancePercentage

	totalCount := 0
	for _, count := range nodeCounts {
		totalCount += count
	}

	for node, count := range nodeCounts {
		percentage := float64(count) / float64(totalCount)
		if percentage < lower || percentage > upper {
			t.Errorf("Node %d has an uneven distribution of keys: %.2f%%. Expected %.2f%% to %.2f%%\n", node, percentage*100, lower*100, upper*100)
		} else {
			t.Logf("Node %d: (%.2f%%)\n", node, percentage*100)
		}
	}
}
