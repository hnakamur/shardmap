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
	var h uintptr
	switch k := key.(type) {
	case nil:
		h = nilinterhash(unsafe.Pointer(&k), 0)
	case bool:
		h = memhash(unsafe.Pointer(&k), 0, unsafe.Sizeof(false))
	case int:
		h = memhash(unsafe.Pointer(&k), 0, unsafe.Sizeof(int(0)))
	case uint:
		h = memhash(unsafe.Pointer(&k), 0, unsafe.Sizeof(uint(0)))
	case uintptr:
		h = memhash(unsafe.Pointer(&k), 0, unsafe.Sizeof(uintptr(0)))
	case int8:
		h = memhash(unsafe.Pointer(&k), 0, unsafe.Sizeof(int8(0)))
	case uint8:
		h = memhash(unsafe.Pointer(&k), 0, unsafe.Sizeof(uint8(0)))
	case int16:
		h = memhash(unsafe.Pointer(&k), 0, unsafe.Sizeof(int16(0)))
	case uint16:
		h = memhash(unsafe.Pointer(&k), 0, unsafe.Sizeof(uint16(0)))
	case int32:
		h = memhash(unsafe.Pointer(&k), 0, unsafe.Sizeof(int32(0)))
	case uint32:
		h = memhash(unsafe.Pointer(&k), 0, unsafe.Sizeof(uint32(0)))
	case int64:
		h = memhash(unsafe.Pointer(&k), 0, unsafe.Sizeof(int64(0)))
	case uint64:
		h = memhash(unsafe.Pointer(&k), 0, unsafe.Sizeof(uint64(0)))
	case float32:
		h = f32hash(unsafe.Pointer(&k), 0)
	case float64:
		h = f64hash(unsafe.Pointer(&k), 0)
	case complex64:
		h = c64hash(unsafe.Pointer(&k), 0)
	case complex128:
		h = c128hash(unsafe.Pointer(&k), 0)
	case string:
		h = strhash(unsafe.Pointer(&k), 0)
	case []byte:
		h = memhash(unsafe.Pointer(&k), 0, uintptr(len(k)))
	default:
		panic("unsupported key type in shardmap.Map")
	}
	return int(h & uintptr(m.shards-1))
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

//go:noescape
//go:linkname nilinterhash runtime.nilinterhash
func nilinterhash(p unsafe.Pointer, h uintptr) uintptr

//go:noescape
//go:linkname f32hash runtime.f32hash
func f32hash(p unsafe.Pointer, h uintptr) uintptr

//go:noescape
//go:linkname f64hash runtime.f64hash
func f64hash(p unsafe.Pointer, h uintptr) uintptr

//go:noescape
//go:linkname c64hash runtime.c64hash
func c64hash(p unsafe.Pointer, h uintptr) uintptr

//go:noescape
//go:linkname c128hash runtime.c128hash
func c128hash(p unsafe.Pointer, h uintptr) uintptr

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

//go:noescape
//go:linkname strhash runtime.strhash
func strhash(p unsafe.Pointer, h uintptr) uintptr
