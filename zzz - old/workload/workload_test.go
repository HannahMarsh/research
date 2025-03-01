package workload

import (
	"benchmark/cache"
	bconfig "benchmark/config"
	"context"
	"testing"
	"time"
)

func TestWorkload(t *testing.T) {
	actualNodes := 4
	virtualNodes := 50000
	nodeRing := cache.NewNodeRing(actualNodes, virtualNodes)

	p := bconfig.GetDefaultConfig()
	p.Workload.ThreadCount.Value = 200
	p.Workload.NumUniqueKeys.Value = 10000
	wLoad, err := NewWorkload(&p, time.Now().Add(time.Duration(1)*time.Second))
	var keys = make(map[string]int)
	var nodes = make(map[string]int)
	var numKeys = 0
	var numGenerated = 0
	if err != nil {
		panic(err)
	}
	var nodeRequestHistogram = make(map[int]int)
	for i := 0; i < actualNodes; i++ {
		nodeRequestHistogram[i] = 0
	}
	var nodeUniqueKeysHistogram = make(map[int]int)
	for i := 0; i < actualNodes; i++ {
		nodeUniqueKeysHistogram[i] = 0
	}

	for i := 0; i < p.Workload.ThreadCount.Value; i++ {

		ctx, _ := context.WithCancel(context.Background())
		w := NewWorker(&p, i, wLoad, nil, nil)
		ctx = wLoad.InitThread(ctx, i, 800, 10000)

		for w.opCount == 0 || w.opsDone < w.opCount {
			opsCount := 1
			state := ctx.Value(stateKey).(*State)
			for j := 0; j < opsCount; j++ {
				key := w.workload.buildKeyName(w.workload.nextKeyNum(state))
				if _, exists := keys[key]; !exists {
					keys[key] = 0
					nodes[key] = nodeRing.GetNode(key)
					nodeUniqueKeysHistogram[nodes[key]]++
					numKeys++
				}
				keys[key]++
				nodeRequestHistogram[nodeRing.GetNode(key)]++
				numGenerated++
			}

			w.opsDone += int64(opsCount)
		}
	}

	maxPop := 0

	for _, count := range keys {
		//t.Logf("key: %s, count: %d", key, count)
		maxPop = max(maxPop, count)
	}
	t.Logf("%d unique keys\n%d generated\naverage key popularity: %2.f\nmax popularity: %d\n", numKeys, numGenerated, float64(numGenerated)/float64(numKeys), maxPop)

	var histogram = make(map[int]int)

	for _, count := range keys {
		if _, exists := histogram[count]; !exists {
			histogram[count] = 0
		}
		histogram[count]++
	}

	for i := 1; i <= maxPop; i++ {
		if _, exists := histogram[i]; !exists {
			histogram[i] = 0
		}
		t.Logf("%d keys with popularity %d\n", histogram[i], i)
	}

	for node, count := range nodeRequestHistogram {
		t.Logf("Node %d has %d requests, %d unique keys\n", node, count, nodeUniqueKeysHistogram[node])
	}

	//if got := c.buildKeyName(tt.args.keyNum); got != tt.want {
	//	t.Errorf("buildKeyName() = %v, want %v", got, tt.want)
	//}
}
