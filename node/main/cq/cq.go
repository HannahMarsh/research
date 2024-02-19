package cq

import (
	"fmt"
	"sort"
	"sync"
)

type Data struct {
	Key         string
	Value       map[string][]byte
	Index       int
	PrimaryNode int
	BackUpNode  int
}

type qNode struct {
	next  *qNode
	prev  *qNode
	data  Data
	index int
}

func (qn *qNode) toString() string {
	next := ""
	prev := ""
	if qn.next != nil {
		next = fmt.Sprintf("next=%s", qn.next.data.Key)
	}
	if qn.prev != nil {
		prev = fmt.Sprintf("prev=%s", qn.prev.data.Key)
	}
	if next != "" && prev != "" {
		next += ", "
	}
	return fmt.Sprintf("%s: [%d],  %s%s", qn.data.Key, qn.index, next, prev)
}

type CQ struct {
	top      *qNode
	bottom   *qNode
	nodes    sync.Map
	keyLocks sync.Map
	maxSize  int
	isFull_  bool
	size     int
	sizeLock sync.RWMutex
	topMu    sync.RWMutex
	bottomMu sync.RWMutex
	lockNMu  sync.RWMutex
}

func NewConcurrentQueue(maxSize int) *CQ {
	return &CQ{
		maxSize: maxSize,
	}
}

func (cq *CQ) Size() int {
	cq.sizeLock.RLock()
	defer cq.sizeLock.RUnlock()
	return cq.size
}

// assume that n is not part of list
func (cq *CQ) insertAtTop(n *qNode) {
	if n == nil {
		panic("insertAtTop called on nil")
	}

	cq.topMu.Lock()
	cq.bottomMu.Lock()
	defer cq.topMu.Unlock()
	defer cq.bottomMu.Unlock()

	unlockFunc := cq.lock(cq.top, n)
	defer unlockFunc()

	if cq.top == nil {
		cq.top = n
		return
	} else {
		cq.top.next = n
		n.prev = cq.top
		if cq.bottom == nil {
			cq.bottom = cq.top
		}
		cq.top = n
	}
}

func (cq *CQ) moveNodeToTop(n *qNode) {
	if n == nil {
		panic("moveNodeToFront called on nil")
	}
	cq.topMu.Lock()
	cq.bottomMu.Lock()
	defer cq.topMu.Unlock()
	defer cq.bottomMu.Unlock()

	unlockFunc := cq.lock(cq.top, cq.bottom, n, n.next, n.prev)
	defer unlockFunc()

	if cq.top == n {
		return
	}
	if cq.top == nil {
		panic("top is nil")
	}

	if cq.bottom == n {
		if cq.bottom.next == cq.top {
			cq.bottom = cq.top
			cq.top = n
			cq.top.next = nil
			cq.top.prev = cq.bottom
			cq.bottom.next = cq.top
			cq.bottom.prev = nil
			return
		}
		cq.bottom = n.next
		cq.bottom.prev = nil
		cq.top.next = n
		n.prev = cq.top
		n.next = nil
		cq.top = n
		return
	} else {
		if n.next != nil {
			n.next.prev = n.prev
		}
		if n.prev != nil {
			n.prev.next = n.next
		}
		cq.top.next = n
		n.prev = cq.top
		n.next = nil
		cq.top = n
		return
	}
}

func (cq *CQ) moveNodeToBottom(n *qNode) {
	if n == nil {
		panic("moveNodeToBottom called on nil")
	}
	cq.topMu.Lock()
	cq.bottomMu.Lock()
	defer cq.topMu.Unlock()
	defer cq.bottomMu.Unlock()

	unlockFunc := cq.lock(cq.top, cq.bottom, n, n.next, n.prev)
	defer unlockFunc()

	if cq.bottom == n {
		return
	}
	if cq.top == nil {
		panic("top is nil")
	}
	if cq.top == n && cq.bottom == nil {
		return
	}

	if cq.top == n {
		if cq.top.prev == cq.bottom {
			cq.bottom = cq.top
			cq.top = n
			cq.top.next = nil
			cq.top.prev = cq.bottom
			cq.bottom.next = cq.top
			cq.bottom.prev = nil
			return
		}
		cq.top = n.prev
		cq.top.next = nil

		cq.bottom.prev = n
		n.next = cq.bottom
		n.prev = nil
		cq.bottom = n
		return
	} else {
		if n.next != nil {
			n.next.prev = n.prev
		}
		if n.prev != nil {
			n.prev.next = n.next
		}
		cq.bottom.prev = n
		n.next = cq.bottom
		n.prev = nil
		cq.bottom = n
		return
	}
}

func (cq *CQ) getLock(key string) *sync.RWMutex {
	if stored, ok := cq.keyLocks.Load(key); ok {
		if st, ok2 := stored.(*sync.RWMutex); ok2 {
			return st
		} else {
			panic("stored is not *sync.RWMutex")
		}
	}
	v := new(sync.RWMutex)
	*v = sync.RWMutex{}
	stored, _ := cq.keyLocks.LoadOrStore(key, v)
	if st, ok := stored.(*sync.RWMutex); ok {
		return st
	} else {
		panic("stored is not *sync.RWMutex")
	}
}

func (cq *CQ) getOrNewNode(key string) (n *qNode, created bool) {
	unlockFunc := cq.lockKeys(key)
	defer unlockFunc()

	if stored, ok := cq.nodes.Load(key); ok {
		if st, ok2 := stored.(*qNode); ok2 {
			return st, false
		} else {
			panic("stored is not *qNode")
		}
	}
	v := new(qNode)
	*v = qNode{data: Data{Key: key}}
	stored, loaded := cq.nodes.LoadOrStore(key, v)
	if st, ok := stored.(*qNode); ok {
		return st, !loaded
	} else {
		panic("stored is not *qNode")
	}
}

func (cq *CQ) getNode(key string) (n *qNode, exists bool) {
	cq.getLock(key).RLock()
	defer cq.getLock(key).RUnlock()

	if stored, ok := cq.nodes.Load(key); ok {
		if st, ok2 := stored.(*qNode); ok2 {
			return st, true
		} else {
			panic("stored is not *qNode")
		}
	}
	return nil, false
}

// lock locks the provided qNodes based on their keys in alphabetical order.
func (cq *CQ) lock(nodes ...*qNode) (unlockFunc func()) {

	var keys []string
	for _, node := range nodes {
		if node != nil {
			keys = append(keys, node.data.Key)
		}
	}
	unlock := cq.lockKeys(keys...)

	return func() {
		unlock()
	}
}

func (cq *CQ) lockKeys(keys ...string) (unlockFunc func()) {
	cq.lockNMu.Lock()
	defer cq.lockNMu.Unlock()

	if len(keys) == 0 {
		return func() {}
	}

	// Use a map to filter out duplicate keys
	uniqueKeys := make(map[string]struct{})
	for _, key := range keys {
		uniqueKeys[key] = struct{}{}
	}

	// Convert the map back to a slice of unique keys
	keys = make([]string, 0, len(uniqueKeys))
	for key := range uniqueKeys {
		keys = append(keys, key)
	}

	// Sort the unique keys alphabetically to prevent deadlocks
	sort.Strings(keys)

	// Lock by key in sorted order
	lockedMutexes := make([]*sync.RWMutex, 0, len(keys))
	for _, key := range keys {
		mu := cq.getLock(key)
		mu.Lock()
		lockedMutexes = append(lockedMutexes, mu) // Store the locked mutex to unlock later
	}

	// Return a function that unlocks all the mutexes
	return func() {
		for _, mu := range lockedMutexes {
			mu.Unlock()
		}
	}
}

func (cq *CQ) incrementSize() bool {
	cq.sizeLock.Lock()
	defer cq.sizeLock.Unlock() // Ensure the lock is always released

	if cq.size < cq.maxSize {
		cq.size++
		return true
	}
	return false
}

func (cq *CQ) decrementSize() bool {
	cq.sizeLock.Lock()
	defer cq.sizeLock.Unlock() // Ensure the lock is always released

	if cq.size > 0 {
		cq.size--
		return true
	}
	return false
}

func (cq *CQ) enqueue(data Data) (evictedNode *qNode, wasAlreadyTop bool, insertedNew bool) {
	n, insertedNewNode := cq.getOrNewNode(data.Key)

	if insertedNewNode {
		cq.insertAtTop(n)

		if !cq.incrementSize() {
			// need to evict bottom
			return cq.popBottom(), false, true
		}
		return nil, false, true
	} else {
		// move to top
		cq.moveNodeToTop(n)
		return nil, false, false
	}
}

func (cq *CQ) dequeue() (dequeuedNode *qNode) {
	defer func() {
		if dequeuedNode != nil {
			cq.decrementSize()
		}
	}()
	return cq.popBottom()
}

func (cq *CQ) popBottom() *qNode {
	cq.topMu.Lock()
	cq.bottomMu.Lock()
	defer cq.topMu.Unlock()
	defer cq.bottomMu.Unlock()

	if cq.bottom == nil {
		if cq.top == nil {
			return nil
		}
		unlockFunc := cq.lock(cq.top)
		defer unlockFunc()
		top := cq.top
		cq.nodes.Delete(cq.top.data.Key)
		cq.top = nil
		return top
	}
	unlockFunc := cq.lock(cq.bottom, cq.top, cq.bottom.next)
	defer unlockFunc()

	if cq.top == nil {
		return nil
	}

	if cq.bottom == nil {
		top := cq.top
		cq.nodes.Delete(cq.top.data.Key)
		cq.top = nil
		return top
	}

	if cq.bottom.next == cq.top {
		bottom := cq.bottom
		cq.nodes.Delete(bottom.data.Key)
		cq.top.prev = nil
		cq.bottom = nil
		return bottom
	}
	bottom := cq.bottom
	bottom.next.prev = nil
	cq.bottom = bottom.next
	cq.nodes.Delete(bottom.data.Key)
	return bottom
}

func (cq *CQ) Get(key string) (map[string][]byte, bool) {
	if n, exists := cq.getNode(key); exists {
		go cq.moveNodeToTop(n)
		return n.data.Value, true
	} else {
		return nil, false
	}
}

func (cq *CQ) Set(key string, value map[string][]byte, backupNode int) {
	cq.enqueue(Data{Key: key, Value: value, BackUpNode: backupNode})
}

func (cq *CQ) toString() string {
	str := "\t_______________________________________\n"
	str += "\tqueue = "
	cur := cq.top
	if cur == nil {
		str += "[  "
	} else {
		str += "["
	}
	for cur != nil {
		str += fmt.Sprintf("%s, ", cur.data.Key)
		cur = cur.prev
	}
	str += "\t- - - - - - - - - - - - - - - - - - - -\n"
	if cq.top == nil {
		str += fmt.Sprintf("\ttop    -> nil\n")
	}
	cur = cq.top
	for cur != cq.bottom {
		if cur == cq.top {
			str += fmt.Sprintf("\ttop    -> %s\n", cur.toString())
		} else {
			str += fmt.Sprintf("\t          %s\n", cur.toString())
		}
		cur = cur.prev
	}
	if cq.bottom == nil {
		str += fmt.Sprintf("\tbottom -> nil\n")
	} else {
		str += fmt.Sprintf("\tbottom -> %s\n", cq.bottom.toString())
	}
	str += "\t_______________________________________\n"
	return str
}
