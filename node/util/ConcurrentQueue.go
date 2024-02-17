package util

import (
	"fmt"
	"strings"
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

func (cq *ConcurrentQueue) ToString() string {
	return cq.q.toString()
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

// TODO fix all these cases for when qn == bottom or top
func (q *queue_) remove(qn *qNode, removeFromArray bool) {
	if qn == nil {
		panic("remove called on nil")
	}
	if q.top == qn {
		q.top = qn.prev
		if qn.prev != nil {
			qn.prev.next = nil
		}
	} else if q.bottom == qn {
		if q.bottom.next == q.top {
			q.bottom = nil
			q.top.prev = nil
		} else {
			q.bottom = qn.next
			if qn.next != nil {
				qn.next.prev = nil
			}
		}
	} else {
		if qn.prev != nil {
			qn.prev.next = qn.next
		}
		if qn.next != nil {
			qn.next.prev = qn.prev
		}
	}
	if removeFromArray {
		q.nodes[qn.index] = nil
		if _, exists := q.hash[qn.data.Key]; exists {
			delete(q.hash, qn.data.Key)
		}
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
		panic("top is nil")
	}
	q.remove(qn, false)
	qn.prev = q.top
	qn.next = nil
	if q.top != nil {
		q.top.next = qn
	}
	q.top = qn

	if q.top.prev != nil && q.top.prev.prev == nil {
		q.bottom = q.top.prev
	}
}

func (q *queue_) moveNodeToBottom(qn *qNode) {
	if qn == nil {
		panic("moveNodeToBottom called on nil")
	}
	if q.top == nil {
		panic("top is nil")
	}
	if q.bottom == qn {
		return
	}
	if q.top == qn && q.bottom == nil {
		return
	}
	q.remove(qn, false)

	if q.bottom == nil {
		qn.next = q.top
		q.top.prev = qn
	} else {
		qn.next = q.bottom
		q.bottom.prev = qn
	}
	qn.prev = nil
	q.bottom = qn
}

func (q *queue_) swap(qn1 *qNode, qn2 *qNode) {
	if qn1 == nil || qn2 == nil {
		panic("swap called on nil")
	}
	if qn1 == qn2 {
		return
	}
	prev1 := qn1.prev
	next1 := qn1.next
	prev2 := qn2.prev
	next2 := qn2.next

	// TODO store these values ahead of time before swpapping becuase qn1 could be infron or behind q2 directly
	if prev1 != nil {
		prev1.next = qn2
	}
	if next1 != nil {
		next1.prev = qn2
	}
	if prev2 != nil {
		prev2.next = qn1
	}
	if next2 != nil {
		next2.prev = qn1
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
	q.top.next = nil
	if q.bottom == qn1 {
		q.bottom = qn2
	} else if q.bottom == qn2 {
		q.bottom = qn1
	}
	q.bottom.prev = nil
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
		if q.top.data.Key == data.Key {
			q.top.data = data
			return nil
		}
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
		q.moveNodeToTop(n)
		//fmt.Printf("\nAfter moving to top: \n%s\n", q.toString())
		return nil
	} else {
		n = &qNode{data: data, index: q.findFirstNilIndex()}
		if n.index >= 0 {
			q.hash[data.Key] = n.index
			q.nodes[n.index] = n
			q.moveNodeToTop(n)
			//fmt.Printf("\nAfter moving to top: \n%s\n", q.toString())
			return nil
		} else {
			bottom := q.bottom
			q.swap(n, bottom)
			//fmt.Printf("\nAfter swapping bottom: \n%s\n", q.toString())
			q.nodes[bottom.index] = n
			n.index = bottom.index
			q.hash[data.Key] = n.index
			//fmt.Printf("\nAfter replacing index: \n%s\n", q.toString())
			q.moveNodeToTop(n)
			//fmt.Printf("\nAfter moving to top: \n%s\n", q.toString())
			return bottom
		}
	}
}

func (q *queue_) dequeue() *qNode {
	bottom := q.popBottom()
	if bottom == nil {
		return nil
	}
	return bottom
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

func (q *queue_) popBottom() *qNode {
	if q.bottom == nil {
		if q.top == nil {
			return nil
		}
		top := q.top
		q.remove(top, true)
		return top
	}
	bottom := q.bottom
	q.remove(bottom, true)
	return bottom
}

func (q *queue_) popTop() *qNode {
	if q.top == nil {
		return nil
	}
	top := q.top
	q.remove(top, true)
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

func (q *queue_) toString() string {
	str := "\t_______________________________________\n"
	str += "\tqueue = "
	cur := q.top
	if cur == nil {
		str += "[  "
	} else {
		str += "["
	}
	for cur != nil {
		str += fmt.Sprintf("%s, ", cur.data.Key)
		cur = cur.prev
	}
	str += fmt.Sprintf("\b\b]\n\thash  = {%s}\n", mapToString(q.hash))
	str += "\t- - - - - - - - - - - - - - - - - - - -\n"
	if q.top == nil {
		str += fmt.Sprintf("\ttop    -> nil\n")
	}
	cur = q.top
	for cur != q.bottom {
		if cur == q.top {
			str += fmt.Sprintf("\ttop    -> %s\n", cur.toString())
		} else {
			str += fmt.Sprintf("\t          %s\n", cur.toString())
		}
		cur = cur.prev
	}
	if q.bottom == nil {
		str += fmt.Sprintf("\tbottom -> nil\n")
	} else {
		str += fmt.Sprintf("\tbottom -> %s\n", q.bottom.toString())
	}
	str += "\t_______________________________________\n"
	return str
}

func mapToString(m map[string]int) string {
	var b strings.Builder
	for key, value := range m {
		b.WriteString(fmt.Sprintf("%s: %d, ", key, value))
	}
	// Remove the last comma and space if the map is not empty
	if b.Len() > 0 {
		return b.String()[:b.Len()-2]
	}
	return b.String()
}
