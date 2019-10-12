package shardmap

import (
	"runtime"
	"sync"
	"unsafe"
)

// Map is a hashmap. Like map[string]interface{}, but sharded and thread-safe.
type Map struct {
	cap    int
	shards int
	seed   uint32
	mus    []sync.RWMutex
	maps   []map[string]interface{}
}

// New returns a new hashmap with the specified capacity. This function is only
// needed when you must define a minimum capacity, otherwise just use:
//    m := New(0)
func New(cap int) *Map {
	m := &Map{cap: cap}
	m.initDo()
	return m
}

// Clear out all values from map
func (m *Map) Clear() {
	for i := 0; i < m.shards; i++ {
		m.mus[i].Lock()
		m.maps[i] = make(map[string]interface{}, m.cap/m.shards)
		m.mus[i].Unlock()
	}
}

// Set assigns a value to a key.
// Returns the previous value, or false when no value was assigned.
func (m *Map) Set(key string, value interface{}) (prev interface{}, replaced bool) {
	shard := m.choose(key)
	m.mus[shard].Lock()
	prev, replaced = m.maps[shard][key]
	m.maps[shard][key] = value
	m.mus[shard].Unlock()
	return prev, replaced
}

// SetAccept assigns a value to a key. The "accept" function can be used to
// inspect the previous value, if any, and accept or reject the change.
// It's also provides a safe way to block other others from writing to the
// same shard while inspecting.
// Returns the previous value, or false when no value was assigned.
func (m *Map) SetAccept(
	key string, value interface{},
	accept func(prev interface{}, replaced bool) bool,
) (prev interface{}, replaced bool) {
	shard := m.choose(key)
	m.mus[shard].Lock()
	defer m.mus[shard].Unlock()
	prev, replaced = m.maps[shard][key]
	m.maps[shard][key] = value
	if accept != nil {
		if !accept(prev, replaced) {
			// revert unaccepted change
			if !replaced {
				// delete the newly set data
				delete(m.maps[shard], key)
			} else {
				// reset updated data
				m.maps[shard][key] = prev
			}
			prev, replaced = nil, false
		}
	}
	return prev, replaced
}

// Get returns a value for a key.
// Returns false when no value has been assign for key.
func (m *Map) Get(key string) (value interface{}, ok bool) {
	shard := m.choose(key)
	m.mus[shard].RLock()
	value, ok = m.maps[shard][key]
	m.mus[shard].RUnlock()
	return value, ok
}

// Delete deletes a value for a key.
// Returns the deleted value, or false when no value was assigned.
func (m *Map) Delete(key string) (prev interface{}, deleted bool) {
	shard := m.choose(key)
	m.mus[shard].Lock()
	prev, deleted = m.maps[shard][key]
	delete(m.maps[shard], key)
	m.mus[shard].Unlock()
	return prev, deleted
}

// DeleteAccept deletes a value for a key. The "accept" function can be used to
// inspect the previous value, if any, and accept or reject the change.
// It's also provides a safe way to block other others from writing to the
// same shard while inspecting.
// Returns the deleted value, or false when no value was assigned.
func (m *Map) DeleteAccept(
	key string,
	accept func(prev interface{}, replaced bool) bool,
) (prev interface{}, deleted bool) {
	shard := m.choose(key)
	m.mus[shard].Lock()
	defer m.mus[shard].Unlock()
	prev, deleted = m.maps[shard][key]
	delete(m.maps[shard], key)
	if accept != nil {
		if !accept(prev, deleted) {
			// revert unaccepted change
			if deleted {
				// reset updated data
				m.maps[shard][key] = prev
			}
			prev, deleted = nil, false
		}
	}

	return prev, deleted
}

// Len returns the number of values in map.
func (m *Map) Len() int {
	var l int
	for i := 0; i < m.shards; i++ {
		m.mus[i].Lock()
		l += len(m.maps[i])
		m.mus[i].Unlock()
	}
	return l
}

// Range iterates overall all key/values.
// It's not safe to call or Set or Delete while ranging.
func (m *Map) Range(iter func(key string, value interface{}) bool) {
	var done bool
	for i := 0; i < m.shards; i++ {
		func() {
			m.mus[i].RLock()
			defer m.mus[i].RUnlock()
			for key, value := range m.maps[i] {
				if !iter(key, value) {
					done = true
					break
				}
			}
		}()
		if done {
			break
		}
	}
}

func (m *Map) choose(key string) int {
	return int(memHashString(key) & uint64(m.shards-1))
}

func (m *Map) initDo() {
	m.shards = 1
	for m.shards < runtime.NumCPU()*16 {
		m.shards *= 2
	}
	scap := m.cap / m.shards
	m.mus = make([]sync.RWMutex, m.shards)
	m.maps = make([]map[string]interface{}, m.shards)
	for i := 0; i < len(m.maps); i++ {
		m.maps[i] = make(map[string]interface{}, scap)
	}
}

type stringStruct struct {
	str unsafe.Pointer
	len int
}

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

// MemHashString is the hash function used by go map, it utilizes available hardware instructions
// (behaves as aeshash if aes instruction is available).
// NOTE: The hash seed changes for every process. So, this cannot be used as a persistent hash.
func memHashString(str string) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&str))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}
