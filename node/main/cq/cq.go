package cq

import (
	"fmt"
	"node/main/util"
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

func (cq *CQ) incrSize() {
	cq.sizeLock.Lock()
	defer cq.sizeLock.Unlock()
	cq.size++
}

func (cq *CQ) decrSize() {
	cq.sizeLock.Lock()
	defer cq.sizeLock.Unlock()
	cq.size--
}

func (cq *CQ) remove(qn *qNode, removeFromArray bool) {
	if qn == nil {
		panic("remove called on nil")
	}
	if cq.top == qn {
		cq.top = qn.prev
		if qn.prev != nil {
			qn.prev.next = nil
		}
	} else if cq.bottom == qn {
		if cq.bottom.next == cq.top {
			cq.bottom = nil
			cq.top.prev = nil
		} else {
			cq.bottom = qn.next
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
		cq.nodes.Delete(qn.data.Key)
	}
}

// TODO make this thread safe
func (cq *CQ) moveNodeToTop(qn *qNode) {
	if qn == nil {
		panic("moveNodeToFront called on nil")
	}
	if cq.top == qn {
		return
	}
	if cq.top == nil {
		panic("top is nil")
	}
	cq.remove(qn, false)
	qn.prev = cq.top
	qn.next = nil
	if cq.top != nil {
		cq.top.next = qn
	}
	cq.top = qn

	if cq.top.prev != nil && cq.top.prev.prev == nil {
		cq.bottom = cq.top.prev
	}
}

func (cq *CQ) moveNodeToBottom(qn *qNode) {
	if qn == nil {
		panic("moveNodeToBottom called on nil")
	}
	if cq.top == nil {
		panic("top is nil")
	}
	if cq.bottom == qn {
		return
	}
	if cq.top == qn && cq.bottom == nil {
		return
	}
	cq.remove(qn, false)

	if cq.bottom == nil {
		qn.next = cq.top
		cq.top.prev = qn
	} else {
		qn.next = cq.bottom
		cq.bottom.prev = qn
	}
	qn.prev = nil
	cq.bottom = qn
}

func (cq *CQ) swap(qn1 *qNode, qn2 *qNode) {
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

	if cq.top == qn1 {
		cq.top = qn2
	} else if cq.top == qn2 {
		cq.top = qn1
	}
	cq.top.next = nil
	if cq.bottom == qn1 {
		cq.bottom = qn2
	} else if cq.bottom == qn2 {
		cq.bottom = qn1
	}
	cq.bottom.prev = nil
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
	if stored, ok := cq.nodes.Load(key); ok {
		if st, ok2 := stored.(*qNode); ok2 {
			return st, false
		} else {
			panic("stored is not *qNode")
		}
	}
	v := new(qNode)
	*v = qNode{data: Data{Key: key}}
	stored, loaded := cq.keyLocks.LoadOrStore(key, v)
	if st, ok := stored.(*qNode); ok {
		return st, !loaded
	} else {
		panic("stored is not *qNode")
	}
}

func (cq *CQ) lock(key string) {
	cq.getLock(key).Lock()
}

func (cq *CQ) lockBoth(key1, key2 string) {
	if key1 == key2 {
		cq.getLock(key1).Lock()
	} else if key1 < key2 {
		cq.getLock(key1).Lock()
		cq.getLock(key2).Lock()
	} else {
		cq.getLock(key2).Lock()
		cq.getLock(key1).Lock()
	}
}

func (cq *CQ) unlockBoth(key1, key2 string) {
	if key1 == key2 {
		cq.getLock(key1).Unlock()
	} else if key1 < key2 {
		cq.getLock(key1).Unlock()
		cq.getLock(key2).Unlock()
	} else {
		cq.getLock(key2).Unlock()
		cq.getLock(key1).Unlock()
	}
}

func (cq *CQ) unlock(key string) {
	cq.getLock(key).Unlock()
}

func (cq *CQ) rLock(key string) {
	cq.getLock(key).RLock()
}

func (cq *CQ) rUnlock(key string) {
	cq.getLock(key).RUnlock()
}

func (cq *CQ) incrementSize() bool {
	cq.sizeLock.RLock()
	if cq.size < cq.maxSize {
		cq.sizeLock.RUnlock()
		cq.sizeLock.Lock()
		if cq.size < cq.maxSize {
			cq.size++
			return true
		} else {
			cq.sizeLock.Unlock()
		}
	} else {
		cq.sizeLock.RUnlock()
	}
	return false
}

// assume that n is not part of list
func (cq *CQ) insertAtTop(n *qNode) {
	if n == nil {
		panic("insertAtTop called on nil")
	}
	if n.next != nil || n.prev != nil {
		panic("insertAtTop called on node that is part of list")
	}

	cq.topMu.Lock()
	cq.bottomMu.Lock()
	defer cq.topMu.Unlock()
	defer cq.bottomMu.Unlock()

	if cq.top == nil {
		cq.top = n
		cq.topMu.Unlock()
		return
	} else {
		k1, k2 := n.data.Key, cq.top.data.Key
		cq.lockBoth(k1, k2)
		cq.top.next = n
		n.prev = cq.top
		if cq.bottom == nil {
			cq.bottom = cq.top
		}
		cq.top = n
		cq.unlockBoth(k1, k2)
	}
}

func (cq *CQ) enqueue(data Data) (evictedNode *qNode, wasAlreadyTop bool, insertedNew bool) {

	//cq.lock(data.Key)
	//
	//defer func() {
	//	// inserted a new one and didn't evict (otherwise size stays the same)
	//	if insertedNew {
	//		//cq.nodes.Store(data.Key, data)
	//		if evictedNode == nil {
	//			cq.incrSize()
	//		}
	//	}
	//	//cq.unlock(data.Key)
	//}()
	cq.lock(data.Key)

	n, insertedNewNode := cq.getOrNewNode(data.Key)

	if insertedNewNode {
		cq.unlock(data.Key)
		cq.insertAtTop(n)

		if !cq.incrementSize() {
			// need to evict bottom
			return cq.popBottom(), false, true
		}
		return nil, false, true
	} else {
		// move to top
		cq.unlock(data.Key)
		cq.moveNodeToTop(n)
		return nil, false, false
	}

	if cq.top == nil {
		n := &qNode{data: data}
		cq.top = n
		cq.bottom = nil
		cq.nodes.Store(data.Key, n)
		return nil, false, true
	}

	if cq.top.data.Key == data.Key {
		cq.top.data = data
		return nil, true, false
	}

	if cq.bottom == nil {
		cq.bottom = &qNode{data: data}
		cq.bottom.next = cq.top
		cq.bottom.prev = nil
		cq.top.prev = cq.bottom
		cq.top.next = nil
		cq.swap(cq.top, cq.bottom)
		cq.nodes.Store(data.Key, data)
		return nil, false, true
	}

	var n *qNode
	if index, exists := cq.contains(data.Key); exists {
		n = cq.nodes[index]
		n.data = data
		cq.moveNodeToTop(n)
		//fmt.Printf("\nAfter moving to top: \n%s\n", q.toString())
		return nil, false, false
	} else {
		n = &qNode{data: data, index: cq.findFirstNilIndex()}
		if n.index >= 0 {
			cq.hash.Store(data.Key, n.index)
			cq.nodes[n.index] = n
			cq.moveNodeToTop(n)
			//fmt.Printf("\nAfter moving to top: \n%s\n", q.toString())
			return nil, false, true
		} else { // need to evict bottom
			bottom := cq.bottom
			cq.swap(n, bottom)
			//fmt.Printf("\nAfter swapping bottom: \n%s\n", q.toString())
			cq.nodes[bottom.index] = n
			cq.hash.Delete(bottom.data.Key)
			n.index = bottom.index
			cq.hash.Store(data.Key, n.index)
			//fmt.Printf("\nAfter replacing index: \n%s\n", q.toString())
			cq.moveNodeToTop(n)
			//fmt.Printf("\nAfter moving to top: \n%s\n", q.toString())
			return bottom, false, true
		}
	}
}

func (cq *CQ) dequeue() (dequeuedNode *qNode) {
	defer func() {
		if dequeuedNode != nil {
			cq.decrSize()
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
		top := cq.top
		cq.lock(cq.top.data.Key)
		cq.nodes.Delete(cq.top.data.Key)
		cq.unlock(cq.top.data.Key)
		cq.top = nil
		return top
	}
	k11, k22 := cq.bottom.data.Key, cq.top.data.Key
	cq.lockBoth(k11, k22)

	if cq.bottom.next == cq.top {
		bottom := cq.bottom
		cq.nodes.Delete(bottom.data.Key)
		cq.top.prev = nil
		cq.unlockBoth(k11, k22)
		cq.bottom = nil
		return bottom
	}
	bottom := cq.bottom
	k1, k2 := bottom.data.Key, bottom.next.data.Key
	cq.unlockBoth(k11, k22)
	cq.lockBoth(k1, k2)
	bottom.next.prev = nil
	cq.bottom = bottom.next
	cq.nodes.Delete(k1)
	cq.unlockBoth(k1, k2)
	return bottom
}

func (cq *CQ) popTop() *qNode {
	if cq.top == nil {
		return nil
	}
	top := cq.top
	cq.remove(top, true)
	return top
}

//func (cq *CQ) contains(key string) (int, bool) {
//	if index, exists := cq.hash[key]; exists {
//		if cq.nodes[index] != nil {
//			if cq.nodes[index].data.Key == key {
//				return index, true
//			}
//		}
//	}
//	return -1, false
//}

func (cq *CQ) isFull() bool {
	return cq.findFirstNilIndex() == -1
}

func (cq *CQ) findFirstNilIndex() int {
	if cq.isFull_ {
		return -1
	}
	for i, n := range cq.nodes {
		if n == nil {
			return i
		}
	}
	cq.isFull_ = true
	return -1
}

func (cq *CQ) Get(key string) (map[string][]byte, bool) {
	cq.rLock(key)
	defer cq.rUnlock(key)

	if index, exists := cq.contains(key); exists && index < cq.maxSize {
		if node := cq.nodes[index]; node != nil {
			cq.moveNodeToTop(node)
			return node.data.Value, true
		}
	}
	return nil, false
}

func (cq *CQ) Set(key string, value map[string][]byte, backupNode int) {
	cq.enqueue(Data{Key: key, Value: value, BackUpNode: backupNode})
}

func (cq *CQ) getTop(n int) map[int]map[string]interface{} {
	m := make(map[int]map[string]interface{})
	count := make(map[int]int)
	for cur := cq.top; cur != nil; cur = cur.prev {
		if _, exists := count[cur.data.BackUpNode]; !exists {
			count[cur.data.BackUpNode] = 0
			m[cur.data.BackUpNode] = make(map[string]interface{})
		}
		if count[cur.data.BackUpNode] >= n {
			continue
		}
		count[cur.data.BackUpNode]++

		m[cur.data.BackUpNode][cur.data.Key] = cur.data.Value
	}
	return m
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
	str += fmt.Sprintf("\b\b]\n\thash  = {%s}\n", util.MapToString(cq.hash))
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
