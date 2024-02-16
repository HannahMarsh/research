package util

import (
	"sync"
)

type Data struct {
	Key   string
	Value interface{}
	Freq  int
	Index int // Index in the heap
	Hash  int
}

type ConcurrentQueue struct {
	q        *queue_
	lock     sync.Mutex
	size     int
	sizeLock sync.RWMutex
	maxSize  int
}

func NewConcurrentQueue(maxSize int) *ConcurrentQueue {
	return &ConcurrentQueue{
		q:       newQueue(maxSize),
		maxSize: maxSize,
	}
}

func (cq *ConcurrentQueue) Size() int {
	cq.sizeLock.RLock()
	defer cq.sizeLock.RUnlock()
	return cq.size
}

func (cq *ConcurrentQueue) Enqueue(data Data) string {
	cq.lock.Lock()
	defer cq.lock.Unlock()
	bottom := cq.q.enqueue(data)
	if bottom != nil {
		return bottom.data.Key
	} else {
		cq.sizeLock.Lock()
		cq.size++
		cq.sizeLock.Unlock()
	}
	return ""
}

func (cq *ConcurrentQueue) Dequeue() Data {
	cq.lock.Lock()
	defer cq.lock.Unlock()
	dq := cq.q.dequeue()
	if dq != nil {
		cq.sizeLock.Lock()
		cq.size--
		cq.sizeLock.Unlock()
		return dq.data
	}
	return Data{}
}

// internal struct (not thread safe):

type qNode struct {
	next  *qNode
	prev  *qNode
	data  Data
	index int
}

type queue_ struct {
	top     *qNode
	bottom  *qNode
	nodes   []*qNode
	maxSize int
	hash    map[string]int
	isFull_ bool
}

func newQueue(maxSize int) *queue_ {
	return &queue_{
		nodes:   make([]*qNode, maxSize),
		hash:    make(map[string]int),
		maxSize: maxSize,
	}
}

func (q *queue_) remove(qn *qNode) {
	if q.top == qn {
		q.top = qn.prev
	}
	if q.bottom == qn {
		q.bottom = qn.next
	}
	if qn.prev != nil {
		qn.prev.next = qn.next
	}
	if qn.next != nil {
		qn.next.prev = qn.prev
	}
	q.nodes[qn.index] = nil
	if _, exists := q.hash[qn.data.Key]; exists {
		delete(q.hash, qn.data.Key)
	}
}

func (q *queue_) moveNodeToTop(qn *qNode) {
	if qn == nil {
		panic("moveNodeToFront called on nil")
	}
	if q.top == qn {
		return
	}
	if q.top == nil {
		q.top = qn
		q.bottom = qn.next
		return
	}
	if qn.prev != nil {
		qn.prev.next = qn.next
	}
	if qn.next != nil {
		qn.next.prev = qn.prev
	}
	qn.next = nil
	qn.prev = q.top
	q.top.next = qn
	q.top = qn
}

func (q *queue_) moveNodeToBottom(qn *qNode) {
	if qn == nil {
		panic("moveNodeToBottom called on nil")
	}
	if q.bottom == qn {
		return
	}
	if q.bottom == nil {
		q.bottom = qn
		q.top = qn.prev
		return
	}
	if qn.prev != nil {
		qn.prev.next = qn.next
	}
	if qn.next != nil {
		qn.next.prev = qn.prev
	}
	qn.prev = nil
	qn.next = q.bottom
	q.bottom.prev = qn
	q.bottom = qn
}

func (q *queue_) swap(qn1 *qNode, qn2 *qNode) {
	if qn1 == nil || qn2 == nil {
		panic("swap called on nil")
	}
	if qn1 == qn2 {
		return
	}
	// TODO store these values ahead of time before swpapping becuase qn1 could be infron or behind q2 directly
	if qn1.prev != nil {
		qn1.prev.next = qn2
	}
	if qn1.next != nil {
		qn1.next.prev = qn2
	}
	if qn2.prev != nil {
		qn2.prev.next = qn1
	}
	if qn2.next != nil {
		qn2.next.prev = qn1
	}

	qn1Prev := qn1.prev
	qn1Next := qn1.next

	qn1.prev, qn1.next = qn2.prev, qn2.next
	qn2.prev, qn2.next = qn1Prev, qn1Next

	if q.top == qn1 {
		q.top = qn2
	} else if q.top == qn2 {
		q.top = qn1
	}
	if q.bottom == qn1 {
		q.bottom = qn2
	} else if q.bottom == qn2 {
		q.bottom = qn1
	}
}

func (q *queue_) enqueue(data Data) *qNode {

	if q.top == nil {
		n := &qNode{data: data, index: q.findFirstNilIndex()}
		if n.index >= 0 {
			q.hash[data.Key] = n.index
			q.nodes[n.index] = n
			q.top = n
			q.bottom = nil
		} else {
			return n
		}
		return nil
	}

	if q.bottom == nil {
		q.bottom = &qNode{data: data, index: q.findFirstNilIndex()}
		if q.bottom.index >= 0 {
			q.hash[data.Key] = q.bottom.index
			q.nodes[q.bottom.index] = q.bottom
			q.bottom.next = q.top
			q.bottom.prev = nil
			q.top.prev = q.bottom
			q.top.next = nil
			q.swap(q.top, q.bottom)
		} else {
			panic("bottom is nil")
		}
	}

	var n *qNode
	if index, exists := q.contains(data.Key); exists {
		n = q.nodes[index]
		n.data = data
	} else {
		n = &qNode{data: data, index: q.findFirstNilIndex()}
		if n.index >= 0 {
			q.hash[data.Key] = n.index
			q.nodes[n.index] = n
		} else {
			bottom := q.popBottom()
			if bottom == nil {
				panic("bottom is nil")
			}
			q.bottom = n
			n.next = bottom.next
			n.prev = nil
			n.next.prev = n
			if q.top == bottom || q.top == nil {
				q.top = n
			}
			q.nodes[bottom.index] = n
			n.index = bottom.index
			q.hash[data.Key] = n.index
			q.moveNodeToTop(n)
			return bottom
		}
	}
	q.moveNodeToTop(n)
	return nil
}

func (q *queue_) dequeue() *qNode {
	bottom := q.popBottom()
	if bottom == nil {
		return nil
	}
	return bottom
}

func (q *queue_) popBottom() *qNode {
	if q.bottom == nil {
		return nil
	}
	bottom := q.bottom
	q.remove(bottom)
	return bottom
}

func (q *queue_) popTop() *qNode {
	if q.top == nil {
		return nil
	}
	top := q.top
	q.remove(top)
	return top
}

func (q *queue_) contains(key string) (int, bool) {
	if index, exists := q.hash[key]; exists {
		if q.nodes[index] != nil {
			if q.nodes[index].data.Key == key {
				return index, true
			}
		}
	}
	return -1, false
}

func (q *queue_) hashNewKey(str string) int {
	if h, exists := q.hash[str]; exists {
		return h
	} else {
		return q.findFirstNilIndex()
	}
}

func (q *queue_) isFull() bool {
	return q.findFirstNilIndex() == -1
}

func (q *queue_) findFirstNilIndex() int {
	if q.isFull_ {
		return -1
	}
	for i, n := range q.nodes {
		if n == nil {
			return i
		}
	}
	q.isFull_ = true
	return -1
}
