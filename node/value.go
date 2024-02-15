package node

import "sync"

type value struct {
	backUpNode    int
	v             map[string][]byte
	accesses      int64
	accessesMutex sync.Mutex
	vMutex        sync.Mutex
}

func (v *value) Set(vv map[string][]byte, backUpNode int) {
	v.vMutex.Lock()
	defer v.vMutex.Unlock()
	v.v = vv
	v.backUpNode = backUpNode
}

func (v *value) SetValueAndAccessCount(vv map[string][]byte, accessCount int64) {
	v.vMutex.Lock()
	v.v = vv
	v.vMutex.Unlock()
	v.accessesMutex.Lock()
	defer v.accessesMutex.Unlock()
	v.accesses = accessCount
}

func (v *value) SetAndIncrement(vv map[string][]byte, backUpNode int) {
	v.increment()
	v.Set(vv, backUpNode)
}

func (v *value) increment() {
	v.accessesMutex.Lock()
	defer v.accessesMutex.Unlock()
	v.accesses++
}

func (v *value) Get() (map[string][]byte, int) {
	v.vMutex.Lock()
	defer v.vMutex.Unlock()
	return v.v, v.backUpNode
}

func (v *value) GetAndIncrement() (map[string][]byte, int) {
	v.increment()
	return v.Get()
}
