package shardmap

import (
	"runtime"
	"sync"
	"unsafe"
)

// Map is a hashmap. Like map[string]interface{}, but sharded and thread-safe.
type Map struct {
	init   sync.Once
	cap    int
	shards int
	seed   uint32
	mus    []sync.RWMutex
	maps   []map[interface{}]interface{}
}

// New returns a new hashmap with the specified capacity. This function is only
// needed when you must define a minimum capacity, otherwise just use:
//    var m Map
func New(cap int) *Map {
	return &Map{cap: cap}
}

// Store sets the value for a key.
func (m *Map) Store(key, value interface{}) {
	m.initDo()
	shard := m.choose(key)
	m.mus[shard].Lock()
	m.maps[shard][key] = value
	m.mus[shard].Unlock()
}

// Load returns the value stored in the map for a key, or nil if no value is present.
// The ok result indicates whether value was found in the map.
func (m *Map) Load(key interface{}) (value interface{}, ok bool) {
	m.initDo()
	shard := m.choose(key)
	m.mus[shard].RLock()
	value, ok = m.maps[shard][key]
	m.mus[shard].RUnlock()
	return value, ok
}

// LoadOrStore returns the existing value for the key if present.
// Otherwise, it stores and returns the given value. The loaded result
// is true if the value was loaded, false if stored.
func (m *Map) LoadOrStore(key, value interface{}) (actual interface{}, loaded bool) {
	m.initDo()
	shard := m.choose(key)
	m.mus[shard].Lock()
	defer m.mus[shard].Unlock()
	actual, loaded = m.maps[shard][key]
	if loaded {
		return actual, true
	}
	m.maps[shard][key] = value
	return value, false
}

// Delete deletes the value for a key.
func (m *Map) Delete(key interface{}) {
	m.initDo()
	shard := m.choose(key)
	m.mus[shard].Lock()
	delete(m.maps[shard], key)
	m.mus[shard].Unlock()
}

// Range calls f sequentially for each key and value present in the map.
// If f returns false, range stops the iteration.
//
// Range does not necessarily correspond to any consistent snapshot of the
// Map's contents: no key will be visited more than once, but if the value
// for any key is stored or deleted concurrently, Range may reflect any
// mapping for that key from any point during the Range call.
//
// Range may be O(N) with the number of elements in the map even if f returns
// false after a constant number of calls.
func (m *Map) Range(iter func(key, value interface{}) bool) {
	m.initDo()
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

func (m *Map) choose(key interface{}) int {
	return int(memHashString(key.(string)) & uint64(m.shards-1))
}

func (m *Map) initDo() {
	m.init.Do(func() {
		m.shards = 1
		for m.shards < runtime.NumCPU()*16 {
			m.shards *= 2
		}
		scap := m.cap / m.shards
		m.mus = make([]sync.RWMutex, m.shards)
		m.maps = make([]map[interface{}]interface{}, m.shards)
		for i := 0; i < len(m.maps); i++ {
			m.maps[i] = make(map[interface{}]interface{}, scap)
		}
	})
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
