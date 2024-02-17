package util

import (
	"fmt"
	"sync"
	"testing"
)

// Mock Data implementation assuming node.Data is a struct with a Key field.
func mockData(key string) Data {
	return Data{Key: key}
}

func TestConcurrentQueueEnqueueDequeue(t *testing.T) {

	fmt.Printf("\n\n")

	cq := NewConcurrentQueue(2) // Small size to test eviction

	// Test Enqueue
	keyEvicted := cq.Enqueue(mockData("key1"))
	fmt.Printf("enqueue(key1): \n%s\n", cq.ToString())
	if keyEvicted != "" {
		t.Errorf("Expected no eviction, but got %s", keyEvicted)
	}

	cq.Enqueue(mockData("key2"))
	fmt.Printf("enqueue(key2): \n%s\n", cq.ToString())

	keyEvicted = cq.Enqueue(mockData("key3"))
	fmt.Printf("enqueue(key3): \n%s\n", cq.ToString())
	if keyEvicted != "key1" {
		t.Errorf("Expected key1 to be evicted, but got %s", keyEvicted)
	}

	// Test Dequeue
	data := cq.Dequeue()
	fmt.Printf("dequeue: \n%s\n", cq.ToString())
	if data.Key != "key2" {
		t.Errorf("Expected key2 to dequeue, got %s", data.Key)
	}

	data = cq.Dequeue()
	fmt.Printf("dequeue: \n%s\n", cq.ToString())
	if data.Key != "key3" {
		t.Errorf("Expected key3 to dequeue, got %s", data.Key)
	}

	data = cq.Dequeue()
	fmt.Printf("dequeue: \n%s\n", cq.ToString())
	if data.Key != "" {
		t.Errorf("Expected empty dequeue, got %s", data.Key)
	}
}

func TestConcurrentQueueConcurrency(t *testing.T) {
	cq := NewConcurrentQueue(100) // Larger queue for concurrency test
	var wg sync.WaitGroup
	numGoroutines := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", id)
			cq.Enqueue(mockData(key))
		}(i)
	}

	wg.Wait()

	// Assuming there's a way to check the size of the queue.
	// This part is pseudo-code since the implementation of size checking is not provided.
	size := cq.Size()
	if size != numGoroutines {
		t.Errorf("Expected size %d, got %d", numGoroutines, size)
	}
}

func TestConcurrentQueueConcurrencyExtended(t *testing.T) {
	cq := NewConcurrentQueue(100) // Adjust as needed for your test scenario
	var wg sync.WaitGroup
	numOps := 200 // Adjust based on how aggressive you want the test to be

	enqueuedItems := make(map[string]bool)
	var enqueuedItemsLock sync.Mutex

	// Concurrently enqueue items
	for i := 0; i < numOps; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", id)
			cq.Enqueue(Data{Key: key, Value: id})
			enqueuedItemsLock.Lock()
			enqueuedItems[key] = true
			enqueuedItemsLock.Unlock()
		}(i)
	}

	// Concurrently dequeue items
	dequeuedItems := make(map[string]bool)
	var dequeuedItemsLock sync.Mutex

	for i := 0; i < numOps/2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data := cq.Dequeue()
			if data.Key != "" { // Assuming an empty Data{} signifies no item was dequeued
				dequeuedItemsLock.Lock()
				dequeuedItems[data.Key] = true
				dequeuedItemsLock.Unlock()
			}
		}()
	}

	wg.Wait()

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
	// Initialize your priority queue
	pq := NewConcurrentQueue(100) // Adjust size as needed

	keys := []string{
		"key3", "key3", "key1", "key2", "key3", "key2", "key3",
	}

	expectedDequeue := []string{
		"key1", "key2", "key3",
	}

	// Enqueue the keys
	for _, key := range keys {
		pq.Enqueue(Data{Key: key, Value: nil})
		fmt.Printf("enqueue(%s): \n%s\n", key, pq.ToString())
	}

	// Dequeue the keys and collect the order
	var dequeuedKeys []string
	for i := 0; i < len(expectedDequeue); i++ {
		data := pq.Dequeue()
		dequeuedKeys = append(dequeuedKeys, data.Key)
	}

	// Verify the dequeue order matches the expected popularity order
	for i, key := range expectedDequeue {
		if dequeuedKeys[i] != key {
			t.Errorf("Expected %s at position %d, got %s", key, i, dequeuedKeys[i])
		}
	}
}
