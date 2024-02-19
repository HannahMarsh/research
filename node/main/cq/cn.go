package cq

import "sync"

type CN struct {
	maxSize  int
	data     []value
	mu       []*sync.RWMutex
	locks    sync.Map
	index    sync.Map
	head     int
	tailKey  string
	headLock sync.RWMutex
}

type value map[string][]byte

func NewCN(maxSize int) *CN {
	cn := &CN{maxSize: maxSize, data: make([]value, 0, maxSize), head: 0, mu: make([]*sync.RWMutex, maxSize)}
	for i := 0; i < maxSize; i++ {
		cn.mu[i] = new(sync.RWMutex)
		cn.mu[i] = &sync.RWMutex{}
	}
	return cn
}

func (cn *CN) swap(i, j int, k1, k2 string) {
	if i == j || k1 == k2 {
		return
	} else if i < j {
		cn.mu[i].Lock()
		cn.mu[j].Lock()
	} else {
		cn.mu[j].Lock()
		cn.mu[i].Lock()
	}
	if k1 < k2 {
		cn.lock(k1)
		cn.lock(k2)
	} else {
		cn.lock(k2)
		cn.lock(k1)
	}

	cn.data[i], cn.data[j] = cn.data[j], cn.data[i]
	cn.index.Store(k1, j)
	cn.index.Store(k2, i)

	cn.mu[i].Unlock()
	cn.mu[j].Unlock()
	cn.unlock(k1)
	cn.unlock(k2)
}

func (cn *CN) swapAndSet(v value, i, j int, k1, k2 string) {
	if i == j || k1 == k2 {
		return
	} else if i < j {
		cn.mu[i].Lock()
		cn.mu[j].Lock()
	} else {
		cn.mu[j].Lock()
		cn.mu[i].Lock()
	}
	if k1 < k2 {
		cn.lock(k1)
		cn.lock(k2)
	} else {
		cn.lock(k2)
		cn.lock(k1)
	}
	cn.data[i] = v

	cn.data[i], cn.data[j] = cn.data[j], cn.data[i]
	cn.index.Store(k1, j)
	cn.index.Store(k2, i)

	cn.mu[i].Unlock()
	cn.mu[j].Unlock()
	cn.unlock(k1)
	cn.unlock(k2)
}

func (cn *CN) Get(key string) map[string][]byte {
	cn.rLock(key)
	defer func() {
		cn.rUnlock(key)
		cn.moveToHead(key)
	}()

	cn.index.Load(key)
	if stored, ok := cn.index.Load(key); ok {
		if st, ok2 := stored.(int); ok2 {
			return cn.data[st]
		} else {
			panic("stored is not value")
		}
	}
	return nil
}

func (cn *CN) moveToHead(key string) {

}

func (cn *CN) Set(key string, v value) {
	cn.headLock.Lock()
	defer func() {
		cn.headLock.Unlock()
	}()

	tail := (cn.head + 1) % cn.maxSize

	cn.lock(key)

	if stored, ok := cn.index.Load(key); ok {
		if st, ok2 := stored.(int); ok2 {
			cn.swapAndSet(v, st, tail, key, cn.tailKey)
			tailKey := cn.tailKey
			cn.head = tail
			return
		} else {
			panic("stored is not int")
		}
	}

	cn.headLock.Lock()
	cn.head = (cn.head + 1) % cn.maxSize
	cn.index.Store(key, cn.head)
	cn.mu[cn.head].Lock()
	cn.data[cn.head] = v
	cn.mu[cn.head].Unlock()

	cn.unlock(key)

	if stored, ok := cn.index.Load(key); ok {
		if st, ok2 := stored.(int); ok2 {
			cn.swap(st, cn.head)

			cn.data[st] = cn.head

			return
		} else {
			panic("stored is not int")
		}
	}

	cn.headLock.Unlock()

}

func (cn *CN) getLock(key string) *sync.RWMutex {
	if stored, ok := cn.locks.Load(key); ok {
		if st, ok2 := stored.(*sync.RWMutex); ok2 {
			return st
		} else {
			panic("stored is not *sync.RWMutex")
		}
	}
	v := new(sync.RWMutex)
	*v = sync.RWMutex{}
	stored, _ := cn.locks.LoadOrStore(key, v)
	if st, ok := stored.(*sync.RWMutex); ok {
		return st
	} else {
		panic("stored is not *sync.RWMutex")
	}
}

func (cn *CN) lock(key string) {
	cn.getLock(key).Lock()
}

func (cn *CN) lockBoth(key1, key2 string) {
	if key1 == key2 {
		cn.getLock(key1).Lock()
	} else if key1 < key2 {
		cn.getLock(key1).Lock()
		cn.getLock(key2).Lock()
	} else {
		cn.getLock(key2).Lock()
		cn.getLock(key1).Lock()
	}
}

func (cn *CN) unlockBoth(key1, key2 string) {
	if key1 == key2 {
		cn.getLock(key1).Unlock()
	} else if key1 < key2 {
		cn.getLock(key1).Unlock()
		cn.getLock(key2).Unlock()
	} else {
		cn.getLock(key2).Unlock()
		cn.getLock(key1).Unlock()
	}
}

func (cn *CN) unlock(key string) {
	cn.getLock(key).Unlock()
}

func (cn *CN) rLock(key string) {
	cn.getLock(key).RLock()
}

func (cn *CN) rUnlock(key string) {
	cn.getLock(key).RUnlock()
}
