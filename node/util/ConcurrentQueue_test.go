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
	cq := NewConcurrentQueue(2) // Small size to test eviction

	// Test Enqueue
	keyEvicted := cq.Enqueue(mockData("key1"))
	if keyEvicted != "" {
		t.Errorf("Expected no eviction, but got %s", keyEvicted)
	}

	cq.Enqueue(mockData("key2"))
	keyEvicted = cq.Enqueue(mockData("key3"))
	if keyEvicted != "key1" {
		t.Errorf("Expected key1 to be evicted, but got %s", keyEvicted)
	}

	// Test Dequeue
	if data := cq.Dequeue(); data.Key != "key2" {
		t.Errorf("Expected key2 to dequeue, got %s", data.Key)
	}
	if data := cq.Dequeue(); data.Key != "key3" {
		t.Errorf("Expected key3 to dequeue, got %s", data.Key)
	}
	if data := cq.Dequeue(); data.Key != "" {
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
