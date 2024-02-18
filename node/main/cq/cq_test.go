package cq

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"
)

// Mock Data implementation assuming node.Data is a struct with a Key field.
func mockData(key string) Data {
	return Data{Key: key}
}

type op struct {
	key                 string
	doEnqueue           bool
	doDequeue           bool
	expectEviction      bool
	expectedEvictedKey  string
	expectNilDequeue    bool
	expectedDequeuedKey string
	testEviction        bool
	testDequeue         bool
	print               bool
	sizeAfter           int
	testSize            bool
}

type enqResult struct {
	nodeEvicted *qNode
	alreadyTop  bool
	insertedNew bool
}

type dqResult struct {
	dequeuedNode *qNode
	wasEmpty     bool
}

type result struct {
	o   op
	enq *enqResult
	dq  *dqResult
}

type results []result

func (rs *results) total(filter func(r result) bool) int {
	total := 0
	for _, r := range *rs {
		if filter(r) {
			total++
		}
	}
	return total
}

func (rs *results) totalEnq(filter func(er enqResult) bool) int {
	return rs.total(func(r result) bool {
		return r.enq != nil && filter(*(r.enq))
	})
}

func (rs *results) totalDq(filter func(dr dqResult) bool) int {
	return rs.total(func(r result) bool {
		return r.dq != nil && filter(*(r.dq))
	})
}

func (o *op) apply(nonDefault op) *op {
	default_ := op{}
	if o.key == default_.key && nonDefault.key != default_.key {
		o.key = nonDefault.key
	}
	if o.doEnqueue == default_.doEnqueue && nonDefault.doEnqueue != default_.doEnqueue {
		o.doEnqueue = nonDefault.doEnqueue
	}
	if o.doDequeue == default_.doDequeue && nonDefault.doDequeue != default_.doDequeue {
		o.doDequeue = nonDefault.doDequeue
	}
	if o.expectEviction == default_.expectEviction && nonDefault.expectEviction != default_.expectEviction {
		o.expectEviction = nonDefault.expectEviction
	}
	if o.expectedEvictedKey == default_.expectedEvictedKey && nonDefault.expectedEvictedKey != default_.expectedEvictedKey {
		o.expectedEvictedKey = nonDefault.expectedEvictedKey
	}
	if o.expectNilDequeue == default_.expectNilDequeue && nonDefault.expectNilDequeue != default_.expectNilDequeue {
		o.expectNilDequeue = nonDefault.expectNilDequeue
	}
	if o.expectedDequeuedKey == default_.expectedDequeuedKey && nonDefault.expectedDequeuedKey != default_.expectedDequeuedKey {
		o.expectedDequeuedKey = nonDefault.expectedDequeuedKey
	}
	if o.testEviction == default_.testEviction && nonDefault.testEviction != default_.testEviction {
		o.testEviction = nonDefault.testEviction
	}
	if o.testDequeue == default_.testDequeue && nonDefault.testDequeue != default_.testDequeue {
		o.testDequeue = nonDefault.testDequeue
	}
	if o.print == default_.print && nonDefault.print != default_.print {
		o.print = nonDefault.print
	}
	if o.sizeAfter == default_.sizeAfter && nonDefault.sizeAfter != default_.sizeAfter {
		o.sizeAfter = nonDefault.sizeAfter
	}
	if o.testSize == default_.testSize && nonDefault.testSize != default_.testSize {
		o.testSize = nonDefault.testSize
	}
	return o
}

func (o *op) test(t *testing.T, cq *CQ) result {
	var er *enqResult
	var dr *dqResult

	if o.doEnqueue {
		nodeEvicted, alreadyTop, insertedNew := cq.enqueue(mockData(o.key))
		er = &enqResult{nodeEvicted: nodeEvicted, alreadyTop: alreadyTop, insertedNew: insertedNew}
		if o.print {
			fmt.Printf("enqueue(%s): \n%s\n", o.key, cq.toString())
		}
		if o.testEviction {
			if nodeEvicted == nil {
				if o.expectEviction {
					t.Errorf("Expected eviction of %s, but got none", o.expectedEvictedKey)
				}
			} else {
				if !o.expectEviction {
					t.Errorf("Expected no eviction, but got %s", nodeEvicted.data.Key)
				} else if nodeEvicted.data.Key != o.expectedEvictedKey {
					t.Errorf("Expected %s to be evicted, but got %s", o.expectedEvictedKey, nodeEvicted.data.Key)
				}
			}
		}
	} else if o.doDequeue {
		nodeDq, wasEmpty := cq.dequeue()
		dr = &dqResult{dequeuedNode: nodeDq, wasEmpty: wasEmpty}
		if o.print {
			fmt.Printf("dequeue(): \n%s\n", cq.toString())
		}
		if o.testDequeue {
			if nodeDq == nil {
				if !o.expectNilDequeue {
					t.Errorf("Expected non-empty dequeue, but got none")
				}
			} else {
				if o.expectNilDequeue {
					t.Errorf("Expected empty dequeue, but got %s", nodeDq.data.Key)
				} else if nodeDq.data.Key != o.expectedDequeuedKey {
					t.Errorf("Expected %s to dequeue, but got %s", o.expectedDequeuedKey, nodeDq.data.Key)
				}
			}
		}
	}
	if o.testSize {
		if size := cq.Size(); size != o.sizeAfter {
			t.Errorf("Expected size %d, got %d", o.sizeAfter, size)
		}
	}
	return result{enq: er, dq: dr, o: *o}
}

func createNOps(n int, o op) []op {
	ops := make([]op, n)
	for i := 0; i < n; i++ {
		ops[i] = o
	}
	return ops
}

func createNOpsWithUniqueKeys(n int, o op) []op {
	return createNOpsWithMUniqueKeys(n, n, o)
}

func createNOpsWithMUniqueKeys(n int, m int, o op) []op {
	ops := createNOps(n, o)
	for i := 0; i < n; i++ {
		ops[i].key = fmt.Sprintf("key%d", i%m)
	}
	return shuffle(ops)
}

func shuffle(ops []op) []op {
	r := rand.New(rand.NewSource(956_248_571))
	r.Shuffle(len(ops), func(i, j int) {
		ops[i], ops[j] = ops[j], ops[i]
	})
	return ops
}

func testSequentialOps(t *testing.T, cq *CQ, ops []op, globalOp op) results {
	fmt.Printf("\n\n")
	rs := make([]result, len(ops))
	for i, o := range ops {
		rs[i] = o.apply(globalOp).test(t, cq)
	}
	return rs
}

func testSequentialOpsWithConditional(t *testing.T, cq *CQ, ops []op, globalOp op, cond func() bool) results {
	fmt.Printf("\n\n")
	var rs []result
	for _, o := range ops {
		if cond() {
			rs = append(rs, o.apply(globalOp).test(t, cq))
		}
	}
	return rs
}

func testConcurrentOps(t *testing.T, cq *CQ, ops []op, globalOp op) results {
	fmt.Printf("\n\n")

	rs := make([]result, len(ops))
	rLock := sync.Mutex{}

	var wg sync.WaitGroup
	numGoroutines := len(ops)

	for i := 0; i < numGoroutines; i++ {
		time.Sleep(time.Duration(time.Now().UnixNano()%10) * time.Microsecond)
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r := ops[id].apply(globalOp).test(t, cq)
			rLock.Lock()
			defer rLock.Unlock()
			rs[id] = r
		}(i)
	}

	wg.Wait()
	return rs
}

func testConcurrentOpsWithConditional(t *testing.T, cq *CQ, ops []op, globalOp op, cond func() bool) results {
	fmt.Printf("\n\n")

	var rs []result
	var rLock sync.Mutex

	var condLock sync.Mutex

	var wg sync.WaitGroup
	numGoroutines := len(ops)

	for i := 0; i < numGoroutines; i++ {
		time.Sleep(time.Duration(time.Now().UnixNano()%10) * time.Microsecond)
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			condLock.Lock()
			if cond() {
				condLock.Unlock()
				r := ops[id].apply(globalOp).test(t, cq)
				rLock.Lock()
				defer rLock.Unlock()
				rs = append(rs, r)
			} else {
				condLock.Unlock()
			}
		}(i)
	}

	wg.Wait()
	return rs
}

func TestSequentialEviction(t *testing.T) {
	testSequentialOps(t, NewConcurrentQueue(2), []op{
		{
			key:       "key1",
			doEnqueue: true,
		}, {
			key:       "key2",
			doEnqueue: true,
		}, {
			key:                "key3",
			doEnqueue:          true,
			expectEviction:     true,
			expectedEvictedKey: "key1",
		}, {
			doDequeue:           true,
			expectedDequeuedKey: "key2",
		}, {
			doDequeue:           true,
			expectedDequeuedKey: "key3",
		}, {
			doDequeue:        true,
			expectNilDequeue: true,
		},
	}, op{
		testEviction: true,
		testDequeue:  true,
		print:        true,
	})
}

func TestSequentialEvictionExtended(t *testing.T) {
	printingEnabled := false
	qSize := 113
	n := 2137 // numOperations
	m := 591  // numUniqueKeys
	cq := NewConcurrentQueue(qSize)

	ops := createNOpsWithMUniqueKeys(n, m, op{doEnqueue: true})

	// enqueue 200 items
	rs := testSequentialOps(t, cq, ops, op{print: printingEnabled})

	totalInsertedNew := rs.totalEnq(func(er enqResult) bool {
		return er.insertedNew
	})

	totalEvicted := rs.totalEnq(func(er enqResult) bool {
		return er.nodeEvicted != nil
	})

	//totalFromTop := rs.totalEnq(func(er enqResult) bool {
	//	return er.alreadyTop
	//})

	totalIncr := rs.totalEnq(func(er enqResult) bool {
		return er.insertedNew && er.nodeEvicted == nil
	})

	if totalIncr != qSize {
		t.Errorf("Expected to increment %d, but instead got %d", qSize, totalIncr)
		return
	}

	if x := rs.totalEnq(func(er enqResult) bool {
		return er.alreadyTop && er.insertedNew
	}); x > 0 {
		t.Errorf("Expected no alreadyTop and insertedNew, but instead got %d", x)
	}

	if x := rs.totalEnq(func(er enqResult) bool {
		return er.alreadyTop && er.nodeEvicted != nil
	}); x > 0 {
		t.Errorf("Expected no alreadyTop and nodeEvicted, but instead got %d", x)
	}

	//fmt.Printf("totalInsertedNew: %d\n", totalFromTop)

	actualSize := cq.Size()

	if qSize != actualSize {
		t.Errorf("Expected full queue at max size %d, but instead got %d", qSize, actualSize)
		return
	}

	if totalInsertedNew < qSize {
		t.Errorf("Expected to insert up to and past a full queue (%d), but instead got %d", qSize, totalInsertedNew)
		return
	}

	if totalInsertedNew-totalEvicted != qSize {
		t.Errorf("Expected to evict %d, but instead got %d", totalInsertedNew-qSize, totalEvicted)
		return
	}

	rs2 := testSequentialOpsWithConditional(t, cq, createNOps(qSize, op{doDequeue: true}), op{print: printingEnabled}, func() bool {
		return cq.Size() > qSize/2
	})

	totalDequeued := rs2.totalDq(func(dr dqResult) bool {
		return dr.dequeuedNode != nil
	})

	remaining := qSize - totalDequeued

	if remaining != cq.Size() {
		t.Errorf("Expected remaining %d, but instead got %d", remaining, cq.Size())
		return
	}
}

func TestConcurrentSizeAfterEnqueues(t *testing.T) {
	numGoroutines := 100
	ops := createNOpsWithUniqueKeys(numGoroutines, op{doEnqueue: true})
	cq := NewConcurrentQueue(numGoroutines)
	testConcurrentOps(t, cq, ops, op{})

	size := cq.Size()
	if size != numGoroutines {
		t.Errorf("Expected size %d, got %d", numGoroutines, size)
	}
}

func TestConcurrentQueueConcurrencyExtended(t *testing.T) {
	printingEnabled := false
	qSize := 113
	n := 2137 // numOperations
	m := 591  // numUniqueKeys
	cq := NewConcurrentQueue(qSize)

	ops := createNOpsWithMUniqueKeys(n, m, op{doEnqueue: true})

	// enqueue 200 items
	rs := testConcurrentOps(t, cq, ops, op{print: printingEnabled})

	totalInsertedNew := rs.totalEnq(func(er enqResult) bool {
		return er.insertedNew
	})

	totalEvicted := rs.totalEnq(func(er enqResult) bool {
		return er.nodeEvicted != nil
	})

	//totalFromTop := rs.totalEnq(func(er enqResult) bool {
	//	return er.alreadyTop
	//})

	totalIncr := rs.totalEnq(func(er enqResult) bool {
		return er.insertedNew && er.nodeEvicted == nil
	})

	if totalIncr != qSize {
		t.Errorf("Expected to increment %d, but instead got %d", qSize, totalIncr)
		return
	}

	if x := rs.totalEnq(func(er enqResult) bool {
		return er.alreadyTop && er.insertedNew
	}); x > 0 {
		t.Errorf("Expected no alreadyTop and insertedNew, but instead got %d", x)
	}

	if x := rs.totalEnq(func(er enqResult) bool {
		return er.alreadyTop && er.nodeEvicted != nil
	}); x > 0 {
		t.Errorf("Expected no alreadyTop and nodeEvicted, but instead got %d", x)
	}

	//fmt.Printf("totalInsertedNew: %d\n", totalFromTop)

	actualSize := cq.Size()

	if qSize != actualSize {
		t.Errorf("Expected full queue at max size %d, but instead got %d", qSize, actualSize)
		return
	}

	if totalInsertedNew < qSize {
		t.Errorf("Expected to insert up to and past a full queue (%d), but instead got %d", qSize, totalInsertedNew)
		return
	}

	if totalInsertedNew-totalEvicted != qSize {
		t.Errorf("Expected to evict %d, but instead got %d", totalInsertedNew-qSize, totalEvicted)
		return
	}

	rs2 := testConcurrentOpsWithConditional(t, cq, createNOps(qSize, op{doDequeue: true}), op{print: printingEnabled}, func() bool {
		return cq.Size() > qSize/2
	})

	totalDequeued := rs2.totalDq(func(dr dqResult) bool {
		return dr.dequeuedNode != nil
	})

	remaining := qSize - totalDequeued

	if remaining != cq.Size() {
		t.Errorf("Expected remaining %d, but instead got %d", remaining, cq.Size())
		return
	}
}

func TestQueuePopularityDequeue(t *testing.T) {

	type data struct {
		keys            []string
		expectedDequeue []string
	}

	d := []data{
		{
			keys:            []string{"key3", "key3", "key1", "key2", "key3", "key2", "key3"},
			expectedDequeue: []string{"key1", "key2", "key3"},
		}, {
			keys:            []string{"key2", "key3", "key1", "key3", "key2", "key3"},
			expectedDequeue: []string{"key1", "key2", "key3"},
		}, {
			keys:            []string{"key3", "key3", "key3", "key2", "key1", "key2", "key1", "key2"},
			expectedDequeue: []string{"key3", "key1", "key2"},
		}, {
			keys:            []string{"key1", "key1", "key1", "key1", "key2", "key3", "key2", "key3", "key3"},
			expectedDequeue: []string{"key1", "key2", "key3"},
		},
		{
			keys:            []string{"key4", "key5", "key5", "key4", "key4", "key5", "key4", "key5", "key4"},
			expectedDequeue: []string{"key5", "key4"},
		},
		{
			keys:            []string{"key6", "key6", "key7", "key6", "key6", "key7", "key7", "key7", "key6"},
			expectedDequeue: []string{"key7", "key6"},
		},
		// Test with some random ordering
		{
			keys:            []string{"key8", "key9", "key8", "key10", "key8", "key9", "key10", "key10"},
			expectedDequeue: []string{"key8", "key9", "key10"},
		},
	}

	for testNum, v := range d {

		keys := v.keys
		expectedDequeue := v.expectedDequeue

		// Initialize your priority queue
		pq := NewConcurrentQueue(100) // Adjust size as needed

		// Enqueue the keys
		for _, key := range keys {
			pq.enqueue(mockData(key))
			//fmt.Printf("enqueue(%s): \n%s\n", key, pq.ToString())
		}

		// Dequeue the keys and collect the order
		var dequeuedKeys []string
		for i := 0; i < len(expectedDequeue); i++ {
			if dqNode, _ := pq.dequeue(); dqNode != nil {
				dequeuedKeys = append(dequeuedKeys, dqNode.data.Key)
			} else {
				t.Errorf("Test%d: Expected non-empty dequeue, but got none", testNum)
			}
		}

		// Verify the dequeue order matches the expected popularity order
		for i, key := range expectedDequeue {
			if dequeuedKeys[i] != key {
				t.Errorf("Test%d: Expected %s at position %d, got %s\n\t\t\t\t\t\t\t\t\texpected: %s\n\t\t\t\t\t\t\t\t\tgot:      %s", testNum, key, i, dequeuedKeys[i], stringArrayToString(expectedDequeue), stringArrayToString(dequeuedKeys))
				return
			}
		}
	}
}

func stringArrayToString(strArr []string) string {
	var b strings.Builder
	for _, str := range strArr {
		b.WriteString(fmt.Sprintf("%s, ", str))
	}
	// Remove the last comma and space if the map is not empty
	if b.Len() > 0 {
		return b.String()[:b.Len()-2]
	}
	return b.String()
}
