package cmap

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// Segment 代表并发安全的散列段的接口。
type Segment interface {
	Put(p Pair) (bool, error)
	Get(key string) Pair
	GetWithHash(key string, keyHash uint64) Pair
	Delete(key string) bool
	Size() uint64
}

// segment 代表并发安全的散列段的类型。
type segment struct {
	// buckets 代表散列桶切片。
	buckets []Bucket
	// bucketsLen 代表散列桶切片的长度。
	bucketsLen int
	// pairTotal 代表键-元素对总数。
	pairTotal uint64
	// pairRedistributor 代表键-元素对的再分布器。
	pairRedistributor PairRedistributor
	lock              sync.Mutex
}

// NewSegment 会创建一个Segment类型的实例。
func newSegment(
	bucketNumber int, pairRedistributor PairRedistributor) Segment {
	if bucketNumber <= 0 {
		bucketNumber = DEFAULT_BUCKET_NUMBER
	}
	if pairRedistributor == nil {
		pairRedistributor =
			newDefaultPairRedistributor(
				DEFAULT_BUCKET_LOAD_FACTOR, bucketNumber)
	}
	buckets := make([]Bucket, bucketNumber)
	for i := 0; i < bucketNumber; i++ {
		buckets[i] = newBucket()
	}
	return &segment{
		buckets:           buckets,
		bucketsLen:        bucketNumber,
		pairRedistributor: pairRedistributor,
	}
}

func (s *segment) Put(p Pair) (bool, error) {
	s.lock.Lock()
	b := s.buckets[int(p.Hash()%uint64(s.bucketsLen))]
	ok, err := b.Put(p, nil)
	if ok {
		newTotal := atomic.AddUint64(&s.pairTotal, 1)
		s.redistribute(newTotal, b.Size())
	}
	s.lock.Unlock()
	return ok, err
}

func (s *segment) Get(key string) Pair {
	return s.GetWithHash(key, hash(key))
}

func (s *segment) GetWithHash(key string, keyHash uint64) Pair {
	s.lock.Lock()
	b := s.buckets[int(keyHash%uint64(s.bucketsLen))]
	s.lock.Unlock()
	return b.Get(key)
}

func (s *segment) Delete(key string) bool {
	s.lock.Lock()
	b := s.buckets[int(hash(key)%uint64(s.bucketsLen))]
	ok := b.Delete(key, nil)
	if ok {
		newTotal := atomic.AddUint64(&s.pairTotal, ^uint64(0))
		s.redistribute(newTotal, b.Size())
	}
	s.lock.Unlock()
	return ok
}

func (s *segment) Size() uint64 {
	return atomic.LoadUint64(&s.pairTotal)
}

func (s *segment) redistribute(pairTotal uint64, bucketSize uint64) (err error) {
	defer func() {
		if p := recover(); p != nil {
			if pErr, ok := p.(error); ok {
				err = newPairRedistributorError(pErr.Error())
			} else {
				err = newPairRedistributorError(fmt.Sprintf("%s", p))
			}
		}
	}()
	s.pairRedistributor.UpdateThreshold(pairTotal, s.bucketsLen)
	bucketStatus := s.pairRedistributor.CheckBucketStatus(pairTotal, bucketSize)
	newBuckets, changed := s.pairRedistributor.Redistribe(bucketStatus, s.buckets)
	if changed {
		s.buckets = newBuckets
		s.bucketsLen = len(s.buckets)
	}
	return nil
}
