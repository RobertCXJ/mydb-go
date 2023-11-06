package common

import (
	"errors"
	"sync"
)

// AbstractCache 实现了一个引用计数策略的缓存
type AbstractCache struct {
	cache       map[int64]interface{}
	references  map[int64]int
	getting     map[int64]bool
	maxResource int
	count       int
	lock        sync.Mutex
	Cache
}

type Cache interface {
	getForCache(int64) (interface{}, error)
	releaseForCache(interface{})
}

// NewAbstractCache 创建一个带有指定 maxResource 的新 AbstractCache
func NewAbstractCache(maxResource int) *AbstractCache {
	return &AbstractCache{
		cache:       make(map[int64]interface{}),
		references:  make(map[int64]int),
		getting:     make(map[int64]bool),
		maxResource: maxResource,
		count:       0,
		lock:        sync.Mutex{},
	}
}

// Get 通过给定的键从缓存中检索元素
func (ac *AbstractCache) Get(key int64) (interface{}, error) {
	for {
		ac.lock.Lock()
		if ac.getting[key] {
			ac.lock.Unlock()
			continue
		}

		if obj, ok := ac.cache[key]; ok {
			ac.references[key]++
			ac.lock.Unlock()
			return obj, nil
		}

		if ac.maxResource > 0 && ac.count == ac.maxResource {
			ac.lock.Unlock()
			return nil, CacheFullError
		}
		ac.count++
		ac.getting[key] = true
		ac.lock.Unlock()
		break
	}

	obj, err := ac.getForCache(key)
	if err != nil {
		ac.lock.Lock()
		ac.count--
		delete(ac.getting, key)
		ac.lock.Unlock()
		return nil, err
	}

	ac.lock.Lock()
	delete(ac.getting, key)
	ac.cache[key] = obj
	ac.references[key] = 1
	ac.lock.Unlock()

	return obj, nil
}

// Release 强制释放缓存条目
func (ac *AbstractCache) Release(key int64) {
	ac.lock.Lock()
	defer ac.lock.Unlock()

	if ref, ok := ac.references[key]; ok {
		ref--
		if ref == 0 {
			obj := ac.cache[key]
			ac.releaseForCache(obj)
			delete(ac.references, key)
			delete(ac.cache, key)
			ac.count--
		} else {
			ac.references[key] = ref
		}
	}
}

// Close 关闭缓存并释放所有资源
func (ac *AbstractCache) Close() {
	ac.lock.Lock()
	defer ac.lock.Unlock()

	for key, obj := range ac.cache {
		ac.releaseForCache(obj)
		delete(ac.references, key)
		delete(ac.cache, key)
		ac.count--
	}
}

// CacheFullError 是指示缓存已满的错误
var CacheFullError = errors.New("cache is full")
