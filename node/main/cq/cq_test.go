package cq

import (
	"fmt"
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

func (o *op) test(t *testing.T, cq *ConcurrentQueue) {
	if o.doEnqueue {
		nodeEvicted, _, _ := cq.enqueue(mockData(o.key))
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
		nodeDq := cq.dequeue()
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
}

func testSequentialOps(t *testing.T, cq *ConcurrentQueue, ops []op, globalOp op) {
	fmt.Printf("\n\n")
	for _, o := range ops {
		o.apply(globalOp).test(t, cq)
	}
}

func testConcurrentOps(t *testing.T, cq *ConcurrentQueue, ops []op, globalOp op) {
	fmt.Printf("\n\n")

	var wg sync.WaitGroup
	numGoroutines := len(ops)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			ops[id].apply(globalOp).test(t, cq)
		}(i)
	}

	wg.Wait()
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
			doEnqueue:           true,
			expectedDequeuedKey: "key2",
		}, {
			doEnqueue:           true,
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

func TestConcurrentQueueConcurrency(t *testing.T) {
	numGoroutines := 100
	ops := make([]op, numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		ops[i] = op{
			key:       fmt.Sprintf("key%d", i),
			doEnqueue: true,
		}
	}

	testConcurrentOps(t, NewConcurrentQueue(100), ops, op{})

	// Assuming there's a way to check the size of the queue.
	// This part is pseudo-code since the implementation of size checking is not provided.
	size := cq.Size()
	if size != numGoroutines {
		t.Errorf("Expected size %d, got %d", numGoroutines, size)
	}
}

func TestConcurrentQueueConcurrencyExtended(t *testing.T) {
	cq := NewConcurrentQueue(500) // Adjust as needed for your test scenario
	var wg sync.WaitGroup
	numOps := 200 // Adjust based on how aggressive you want the test to be

	enqueuedItems := make(map[string]bool)
	var enqueuedItemsLock sync.Mutex

	numEnqueued := 0

	// Concurrently enqueue items
	for i := 0; i < numOps; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			time.Sleep(time.Duration(time.Now().UnixNano()%10) * time.Microsecond)
			key := fmt.Sprintf("key%d", id)
			cq.Enqueue(Data{Key: key, Value: id})
			enqueuedItemsLock.Lock()
			numEnqueued++
			enqueuedItems[key] = true
			enqueuedItemsLock.Unlock()

			//if _, wasAlreadyTop := cq.Enqueue(Data{Key: key, Value: id}); !wasAlreadyTop {
			//	enqueuedItemsLock.Lock()
			//	numEnqueued++
			//	enqueuedItems[key] = true
			//	enqueuedItemsLock.Unlock()
			//} else {
			//	enqueuedItemsLock.Lock()
			//	enqueuedItems[key] = true
			//	enqueuedItemsLock.Unlock()
			//}
		}(i)

	}

	// Concurrently dequeue items
	dequeuedItems := make(map[string]bool)
	var dequeuedItemsLock sync.Mutex

	numDequeued := 0

	for i := 0; i < numOps/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := cq.Dequeue()
			if data.Key != "" { // Assuming an empty Data{} signifies no item was dequeued
				dequeuedItemsLock.Lock()
				dequeuedItems[data.Key] = true
				numDequeued++
				dequeuedItemsLock.Unlock()
			}
		}()
	}

	wg.Wait()

	fmt.Printf("%s", cq.ToString())

	// Verify that the size of the queue is as expected
	actualSize := cq.Size()
	if actualSize < numOps-numOps/2 {
		t.Errorf("Queue size after concurrent operations is less than expected; got %d, want at least %d", actualSize, numOps-numOps/2)
	}

	// After wg.Wait(), verifying the integrity of enqueued and dequeued items.
	for key := range dequeuedItems {
		if !enqueuedItems[key] {
			t.Errorf("Item dequeued that was never enqueued: %s", key)
		}
	}

	// Optional: Verify specific behavior or order if applicable
	// For a simple size check:
	expectedRemaining := len(enqueuedItems) - len(dequeuedItems)
	if expectedRemaining != actualSize {
		t.Errorf("Mismatch in expected and actual remaining items; expected %d, got %d", expectedRemaining, actualSize)
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
			pq.Enqueue(Data{Key: key, Value: nil})
			//fmt.Printf("enqueue(%s): \n%s\n", key, pq.ToString())
		}

		// Dequeue the keys and collect the order
		var dequeuedKeys []string
		for i := 0; i < len(expectedDequeue); i++ {
			data_ := pq.Dequeue()
			dequeuedKeys = append(dequeuedKeys, data_.Key)
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
