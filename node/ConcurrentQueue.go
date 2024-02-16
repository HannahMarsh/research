package node

type QNode struct {
	next  *QNode
	prev  *QNode
	data  Data
	index int
}

type Queue struct {
	top     *QNode
	bottom  *QNode
	nodes   []*QNode
	maxSize int
	hash    map[string]int
	isFull_ bool
}

func NewQueue(maxSize int) *Queue {
	return &Queue{
		nodes:   make([]*QNode, maxSize),
		maxSize: maxSize,
	}
}

func (q *Queue) remove(qn *QNode) {
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
	delete(q.hash, qn.data.key)
}

func (q *Queue) moveNodeToTop(qn *QNode) {
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

func (q *Queue) moveNodeToBottom(qn *QNode) {
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

func (q *Queue) swap(qn1 *QNode, qn2 *QNode) {
	if qn1 == nil || qn2 == nil {
		panic("swap called on nil")
	}
	if qn1 == qn2 {
		return
	}
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

func (q *Queue) Enqueue(data Data) {

	var node *QNode
	if index, exists := q.contains(data.key); exists {
		node = q.nodes[index]
		node.data = data
	} else {
		node = &QNode{data: data, index: q.findFirstNilIndex()}
		if node.index >= 0 {
			q.hash[data.key] = node.index
		} else {
			bottom := q.popBottom()
			q.nodes[bottom.index] = nil
			node.index = bottom.index
			q.hash[data.key] = node.index
		}
	}
	q.moveNodeToTop(node)
}

func (q *Queue) Dequeue() Data {
	bottom := q.popBottom()
	if bottom == nil {
		return Data{}
	}
	return bottom.data
}

func (q *Queue) popBottom() *QNode {
	if q.bottom == nil {
		return nil
	}
	bottom := q.bottom
	q.remove(bottom)
	return bottom
}

func (q *Queue) popTop() *QNode {
	if q.top == nil {
		return nil
	}
	top := q.top
	q.remove(top)
	return top
}

func (q *Queue) contains(key string) (int, bool) {
	if index, exists := q.hash[key]; exists {
		if q.nodes[index] != nil {
			if q.nodes[index].data.key == key {
				return index, true
			}
		}
	}
	return -1, false
}

func (q *Queue) hashNewKey(str string) int {
	if h, exists := q.hash[str]; exists {
		return h
	} else {
		return q.findFirstNilIndex()
	}
}

func (q *Queue) isFull() bool {
	return q.findFirstNilIndex() == -1
}

func (q *Queue) findFirstNilIndex() int {
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
