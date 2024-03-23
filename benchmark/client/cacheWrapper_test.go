package client

import (
	"benchmark/util"
	"fmt"
	"strconv"
	"testing"
)

func TestCacheWrapper_permute(t *testing.T) {

	size := 4
	result := permute(util.CreateArray(size))
	freq := make(map[int]map[int]int)
	for i := 0; i < size; i++ {
		freq[i] = make(map[int]int)
		for j := 0; j < size; j++ {
			freq[i][j] = 0
		}
	}
	for i := 0; i < len(result); i++ {
		//fmt.Printf("\nresult[%d] = %v\n--------------\n", i, result[i])
		for j := 0; j < size; j++ {
			freq[result[i][j]][j] = freq[result[i][j]][j] + 1
			//fmt.Printf("result[%d][%d] = %d, \t", i, j, result[i][j])
			//fmt.Printf("freq[%d][%d] = %d\n", result[i][j], j, freq[result[i][j]][j])
		}
	}

	expectedFreq := int(factorial(int64(size - 1)))

	for num, freqs := range freq {
		for index, frequency := range freqs {

			if frequency != expectedFreq {
				t.Errorf("Node %d comes %s %d times, but expected %d", num, getIndex(index), frequency, expectedFreq)
			} else {
				fmt.Printf("Node %d comes %s %d times\n", num, getIndex(index), frequency)
			}
		}
	}
	fmt.Printf("%v\n", result)
}

func factorial(n int64) (result int64) {
	if n > 0 {
		result = n * factorial(n-1)
		return result
	}
	return 1
}

func getIndex(n int) string {
	if n == 0 {
		return "1st"
	}
	if n == 1 {
		return "2nd"
	}
	if n == 2 {
		return "3rd"
	}
	return strconv.Itoa(n+1) + "th"
}

//
//// TestNodeRingDistribution tests that the distribution of keys hashed by `nodeRing` is approximately even across the nodes.
//func TestNodeRingDistribution(t *testing.T) {
//	// Setup
//	actualNodes := 4
//	virtualNodes := 50000
//	nodeRing := NewNodeRing(actualNodes, virtualNodes)
//
//	// Key generation and distribution tracking
//	keyCount := 50000000 // number of keys to test
//
//	digits := int(math.Ceil(math.Log10(float64(keyCount))))
//	nodeCounts := make(map[int]int)
//	for i := 0; i < keyCount; i++ {
//		str := fmt.Sprintf("%%0%dd", digits)
//		key := fmt.Sprintf(str, i)
//		nodeIndex := nodeRing.GetNode(key)
//		nodeCounts[nodeIndex]++
//	}
//
//	// Analysis
//	expectedPercentage := 1.0 / float64(actualNodes)
//	variancePercentage := 0.02 // 1.5% variance allowed
//
//	lower := expectedPercentage - variancePercentage
//	upper := expectedPercentage + variancePercentage
//
//	totalCount := 0
//	for _, count := range nodeCounts {
//		totalCount += count
//	}
//
//	for node, count := range nodeCounts {
//		percentage := float64(count) / float64(totalCount)
//		if percentage < lower || percentage > upper {
//			t.Errorf("Node %d has an uneven distribution of keys: %.2f%%. Expected %.2f%% to %.2f%%\n", node, percentage*100, lower*100, upper*100)
//		} else {
//			t.Logf("Node %d: (%.2f%%)\n", node, percentage*100)
//		}
//	}
//}
