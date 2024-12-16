package cmap

import (
	"math"
	"sync/atomic"
)

// ConcurrentMap 代表并发安全的字典的接口。
type ConcurrentMap interface {
	Concurrency() int
	Put(key string, element interface{}) (bool, error)
	Get(key string) interface{}
	Delete(key string) bool
	Len() uint64
}

// myConcurrentMap 代表ConcurrentMap接口的实现类型。
type myConcurrentMap struct {
	concurrency int
	segments    []Segment
	total       uint64
}

// NewConcurrentMap 会创建一个ConcurrentMap类型的实例。
// 参数pairRedistributor可以为nil。
func NewConcurrentMap(
	concurrency int,
	pairRedistributor PairRedistributor) (ConcurrentMap, error) {
	if concurrency <= 0 {
		return nil, newIllegalParameterError("concurrency is too small")
	}
	if concurrency > MAX_CONCURRENCY {
		return nil, newIllegalParameterError("concurrency is too large")
	}
	cmap := &myConcurrentMap{}
	cmap.concurrency = concurrency
	cmap.segments = make([]Segment, concurrency)
	for i := 0; i < concurrency; i++ {
		cmap.segments[i] =
			newSegment(DEFAULT_BUCKET_NUMBER, pairRedistributor)
	}
	return cmap, nil
}

func (cmap *myConcurrentMap) Concurrency() int {
	return cmap.concurrency
}

func (cmap *myConcurrentMap) Put(key string, element interface{}) (bool, error) {
	p, err := newPair(key, element)
	if err != nil {
		return false, err
	}
	s := cmap.findSegment(p.Hash())
	ok, err := s.Put(p)
	if ok {
		atomic.AddUint64(&cmap.total, 1)
	}
	return ok, err
}

func (cmap *myConcurrentMap) Get(key string) interface{} {
	keyHash := hash(key)
	s := cmap.findSegment(keyHash)
	pair := s.GetWithHash(key, keyHash)
	if pair == nil {
		return nil
	}
	return pair.Element()
}

func (cmap *myConcurrentMap) Delete(key string) bool {
	s := cmap.findSegment(hash(key))
	if s.Delete(key) {
		atomic.AddUint64(&cmap.total, ^uint64(0))
		return true
	}
	return false
}

func (cmap *myConcurrentMap) Len() uint64 {
	return atomic.LoadUint64(&cmap.total)
}

// findSegment 会根据给定参数寻找并返回对应散列段。
func (cmap *myConcurrentMap) findSegment(keyHash uint64) Segment {
	if cmap.concurrency == 1 {
		return cmap.segments[0]
	}
	var keyHashHigh int
	if keyHash > math.MaxUint32 {
		keyHashHigh = int(keyHash >> 48)
	} else {
		keyHashHigh = int(keyHash >> 16)
	}
	return cmap.segments[keyHashHigh%cmap.concurrency]
}
