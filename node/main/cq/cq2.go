package cq

//import (
//	"fmt"
//	"strings"
//	"sync"
//)
//
//type Data2 struct {
//	Key         string
//	Value       interface{}
//	Index       int
//	PrimaryNode int
//	BackUpNode  int
//}
//
//type ConcurrentQueue2 struct {
//	q        *queue_2
//	lock     sync.Mutex
//	size     int
//	sizeLock sync.RWMutex
//	maxSize  int
//}
//
//func NewConcurrentQueue2(maxSize int) *ConcurrentQueue2 {
//	return &ConcurrentQueue2{
//		q:       newQueue2(maxSize),
//		maxSize: maxSize,
//	}
//}
//
//func (cq *ConcurrentQueue2) Size() int {
//	cq.sizeLock.RLock()
//	defer cq.sizeLock.RUnlock()
//	return cq.size
//}
//
//func (cq *ConcurrentQueue2) incrSize() {
//	cq.sizeLock.Lock()
//	defer cq.sizeLock.Unlock()
//	cq.size++
//}
//
//func (cq *ConcurrentQueue2) decrSize() {
//	cq.sizeLock.Lock()
//	defer cq.sizeLock.Unlock()
//	cq.size--
//}
//
//func (cq *ConcurrentQueue2) Get(key string) (Data2, bool) {
//	cq.lock.Lock()
//	defer cq.lock.Unlock()
//	if d, present := cq.q.get(key); present {
//		cq.q.enqueue(d)
//		return d, true
//	}
//	return Data2{}, false
//}
//
//func (cq *ConcurrentQueue2) Set(key string, d Data2) {
//	d.Key = key
//	cq.Enqueue(d)
//}
//
//func (cq *ConcurrentQueue2) GetTop(n int) map[int]map[string]interface{} {
//	cq.lock.Lock()
//	defer cq.lock.Unlock()
//	return cq.q.getTop(n)
//}
//
//func (cq *ConcurrentQueue2) Enqueue(data Data2) (string, bool) {
//	cq.lock.Lock()
//	defer cq.lock.Unlock()
//	bottom, isAlreadyTop, increaseSize := cq.q.enqueue(data)
//	if increaseSize {
//		cq.sizeLock.Lock()
//		cq.size++
//		cq.sizeLock.Unlock()
//	} else {
//		// fmt.Printf("\nAfter enqueue: \n%s\n", cq.q.toString())
//	}
//	if bottom != nil {
//		return bottom.data.Key, isAlreadyTop
//	}
//	return "", isAlreadyTop
//}
//
//func (cq *ConcurrentQueue2) Dequeue() Data2 {
//	cq.lock.Lock()
//	defer cq.lock.Unlock()
//	dq := cq.q.dequeue()
//	if dq != nil {
//		cq.sizeLock.Lock()
//		cq.size--
//		cq.sizeLock.Unlock()
//		return dq.data
//	}
//	return Data2{}
//}
//
//func (cq *ConcurrentQueue2) ToString() string {
//	return cq.q.toString()
//}
//
//// internal struct (not thread safe):
//
//type qNode2 struct {
//	next  *qNode2
//	prev  *qNode2
//	data  Data2
//	index int
//}
//
//type queue_2 struct {
//	top     *qNode2
//	bottom  *qNode2
//	nodes   []*qNode2
//	maxSize int
//	hash    map[string]int
//	isFull_ bool
//}
//
//func newQueue2(maxSize int) *queue_2 {
//	return &queue_2{
//		nodes:   make([]*qNode2, maxSize),
//		hash:    make(map[string]int),
//		maxSize: maxSize,
//	}
//}
//
//// TODO fix all these cases for when qn == bottom or top
//func (q *queue_2) remove(qn *qNode2, removeFromArray bool) {
//	if qn == nil {
//		panic("remove called on nil")
//	}
//	if q.top == qn {
//		q.top = qn.prev
//		if qn.prev != nil {
//			qn.prev.next = nil
//		}
//	} else if q.bottom == qn {
//		if q.bottom.next == q.top {
//			q.bottom = nil
//			q.top.prev = nil
//		} else {
//			q.bottom = qn.next
//			if qn.next != nil {
//				qn.next.prev = nil
//			}
//		}
//	} else {
//		if qn.prev != nil {
//			qn.prev.next = qn.next
//		}
//		if qn.next != nil {
//			qn.next.prev = qn.prev
//		}
//	}
//	if removeFromArray {
//		q.nodes[qn.index] = nil
//		if _, exists := q.hash[qn.data.Key]; exists {
//			delete(q.hash, qn.data.Key)
//		}
//	}
//}
//
//func (q *queue_2) moveNodeToTop(qn *qNode2) {
//	if qn == nil {
//		panic("moveNodeToFront called on nil")
//	}
//	if q.top == qn {
//		return
//	}
//	if q.top == nil {
//		panic("top is nil")
//	}
//	q.remove(qn, false)
//	qn.prev = q.top
//	qn.next = nil
//	if q.top != nil {
//		q.top.next = qn
//	}
//	q.top = qn
//
//	if q.top.prev != nil && q.top.prev.prev == nil {
//		q.bottom = q.top.prev
//	}
//}
//
//func (q *queue_2) moveNodeToBottom(qn *qNode2) {
//	if qn == nil {
//		panic("moveNodeToBottom called on nil")
//	}
//	if q.top == nil {
//		panic("top is nil")
//	}
//	if q.bottom == qn {
//		return
//	}
//	if q.top == qn && q.bottom == nil {
//		return
//	}
//	q.remove(qn, false)
//
//	if q.bottom == nil {
//		qn.next = q.top
//		q.top.prev = qn
//	} else {
//		qn.next = q.bottom
//		q.bottom.prev = qn
//	}
//	qn.prev = nil
//	q.bottom = qn
//}
//
//func (q *queue_2) swap(qn1 *qNode2, qn2 *qNode2) {
//	if qn1 == nil || qn2 == nil {
//		panic("swap called on nil")
//	}
//	if qn1 == qn2 {
//		return
//	}
//	prev1 := qn1.prev
//	next1 := qn1.next
//	prev2 := qn2.prev
//	next2 := qn2.next
//
//	if prev1 != nil {
//		prev1.next = qn2
//	}
//	if next1 != nil {
//		next1.prev = qn2
//	}
//	if prev2 != nil {
//		prev2.next = qn1
//	}
//	if next2 != nil {
//		next2.prev = qn1
//	}
//
//	qn1Prev := qn1.prev
//	qn1Next := qn1.next
//
//	qn1.prev, qn1.next = qn2.prev, qn2.next
//	qn2.prev, qn2.next = qn1Prev, qn1Next
//
//	if q.top == qn1 {
//		q.top = qn2
//	} else if q.top == qn2 {
//		q.top = qn1
//	}
//	q.top.next = nil
//	if q.bottom == qn1 {
//		q.bottom = qn2
//	} else if q.bottom == qn2 {
//		q.bottom = qn1
//	}
//	q.bottom.prev = nil
//}
//
//func (q *queue_2) enqueue(data Data2) (*qNode2, bool, bool) {
//
//	defer func() {
//
//	}
//
//	if q.top == nil {
//		n := &qNode2{data: data, index: q.findFirstNilIndex()}
//		if n.index >= 0 {
//			q.hash[data.Key] = n.index
//			q.nodes[n.index] = n
//			q.top = n
//			q.bottom = nil
//		} else {
//			panic("top is nil")
//		}
//		return nil, true, true
//	}
//
//	if q.top.data.Key == data.Key {
//		q.top.data = data
//		return nil, false, false
//	}
//
//	if q.bottom == nil {
//		firstNil := q.findFirstNilIndex()
//		q.bottom = &qNode2{data: data, index: firstNil}
//		if q.bottom.index >= 0 {
//			q.hash[data.Key] = q.bottom.index
//			q.nodes[q.bottom.index] = q.bottom
//			q.bottom.next = q.top
//			q.bottom.prev = nil
//			q.top.prev = q.bottom
//			q.top.next = nil
//			q.swap(q.top, q.bottom)
//		} else {
//			panic("bottom is nil")
//		}
//		return nil, true, true
//	}
//
//	var n *qNode2
//	if index, exists := q.contains(data.Key); exists {
//		n = q.nodes[index]
//		n.data = data
//		q.moveNodeToTop(n)
//		//fmt.Printf("\nAfter moving to top: \n%s\n", q.toString())
//		return nil, true, false
//	} else {
//		n = &qNode2{data: data, index: q.findFirstNilIndex()}
//		if n.index >= 0 {
//			q.hash[data.Key] = n.index
//			q.nodes[n.index] = n
//			q.moveNodeToTop(n)
//			//fmt.Printf("\nAfter moving to top: \n%s\n", q.toString())
//			return nil, true, true
//		} else {
//			bottom := q.bottom
//			q.swap(n, bottom)
//			//fmt.Printf("\nAfter swapping bottom: \n%s\n", q.toString())
//			q.nodes[bottom.index] = n
//			delete(q.hash, bottom.data.Key)
//			n.index = bottom.index
//			q.hash[data.Key] = n.index
//			//fmt.Printf("\nAfter replacing index: \n%s\n", q.toString())
//			q.moveNodeToTop(n)
//			//fmt.Printf("\nAfter moving to top: \n%s\n", q.toString())
//			return bottom, true, false
//		}
//	}
//}
//
//func (q *queue_2) dequeue() *qNode2 {
//	bottom := q.popBottom()
//	if bottom == nil {
//		return nil
//	}
//	q.isFull_ = false
//	return bottom
//}
//
//func (qn *qNode2) toString() string {
//	next := ""
//	prev := ""
//	if qn.next != nil {
//		next = fmt.Sprintf("next=%s", qn.next.data.Key)
//	}
//	if qn.prev != nil {
//		prev = fmt.Sprintf("prev=%s", qn.prev.data.Key)
//	}
//	if next != "" && prev != "" {
//		next += ", "
//	}
//	return fmt.Sprintf("%s: [%d],  %s%s", qn.data.Key, qn.index, next, prev)
//}
//
//func (q *queue_2) popBottom() *qNode2 {
//	if q.bottom == nil {
//		if q.top == nil {
//			return nil
//		}
//		top := q.top
//		q.remove(top, true)
//		return top
//	}
//	bottom := q.bottom
//	q.remove(bottom, true)
//	return bottom
//}
//
//func (q *queue_2) popTop() *qNode2 {
//	if q.top == nil {
//		return nil
//	}
//	top := q.top
//	q.remove(top, true)
//	return top
//}
//
//func (q *queue_2) contains(key string) (int, bool) {
//	if index, exists := q.hash[key]; exists {
//		if q.nodes[index] != nil {
//			if q.nodes[index].data.Key == key {
//				return index, true
//			}
//		}
//	}
//	return -1, false
//}
//
//func (q *queue_2) hashNewKey(str string) int {
//	if h, exists := q.hash[str]; exists {
//		return h
//	} else {
//		return q.findFirstNilIndex()
//	}
//}
//
//func (q *queue_2) isFull() bool {
//	return q.findFirstNilIndex() == -1
//}
//
//func (q *queue_2) findFirstNilIndex() int {
//	if q.isFull_ {
//		return -1
//	}
//	for i, n := range q.nodes {
//		if n == nil {
//			return i
//		}
//	}
//	q.isFull_ = true
//	return -1
//}
//
//func (q *queue_2) get(key string) (Data2, bool) {
//	if index, exists := q.contains(key); exists {
//		return q.nodes[index].data, true
//	}
//	return Data2{}, false
//}
//
//func (q *queue_2) set(key string, d Data2) {
//	if index, exists := q.contains(key); exists {
//		q.nodes[index].data = d
//	}
//}
//
////func (q *queue_2) getTop(n int, filter func(Data2)bool) map[string]interface{} {
////	m := make(map[string]interface{})
////	count := 0
////	for cur := q.top; count < n && cur != nil; cur = cur.prev {
////		if filter(cur.data) {
////			m[cur.data.Key] = cur.data.Value
////			count++
////			if count == n {
////				break
////			}
////		}
////	}
////	return m
////}
//
//func (q *queue_2) getTop(n int) map[int]map[string]interface{} {
//	m := make(map[int]map[string]interface{})
//	count := make(map[int]int)
//	for cur := q.top; cur != nil; cur = cur.prev {
//		if _, exists := count[cur.data.BackUpNode]; !exists {
//			count[cur.data.BackUpNode] = 0
//			m[cur.data.BackUpNode] = make(map[string]interface{})
//		}
//		if count[cur.data.BackUpNode] >= n {
//			continue
//		}
//		count[cur.data.BackUpNode]++
//
//		m[cur.data.BackUpNode][cur.data.Key] = cur.data.Value
//	}
//	return m
//}
//
//func (q *queue_2) toString() string {
//	str := "\t_______________________________________\n"
//	str += "\tqueue = "
//	cur := q.top
//	if cur == nil {
//		str += "[  "
//	} else {
//		str += "["
//	}
//	for cur != nil {
//		str += fmt.Sprintf("%s, ", cur.data.Key)
//		cur = cur.prev
//	}
//	str += fmt.Sprintf("\b\b]\n\thash  = {%s}\n", mapToString2(q.hash))
//	str += "\t- - - - - - - - - - - - - - - - - - - -\n"
//	if q.top == nil {
//		str += fmt.Sprintf("\ttop    -> nil\n")
//	}
//	cur = q.top
//	for cur != q.bottom {
//		if cur == q.top {
//			str += fmt.Sprintf("\ttop    -> %s\n", cur.toString())
//		} else {
//			str += fmt.Sprintf("\t          %s\n", cur.toString())
//		}
//		cur = cur.prev
//	}
//	if q.bottom == nil {
//		str += fmt.Sprintf("\tbottom -> nil\n")
//	} else {
//		str += fmt.Sprintf("\tbottom -> %s\n", q.bottom.toString())
//	}
//	str += "\t_______________________________________\n"
//	return str
//}
//
//func mapToString2(m map[string]int) string {
//	var b strings.Builder
//	for key, value := range m {
//		b.WriteString(fmt.Sprintf("%s: %d, ", key, value))
//	}
//	// Remove the last comma and space if the map is not empty
//	if b.Len() > 0 {
//		return b.String()[:b.Len()-2]
//	}
//	return b.String()
//}
