package cmap

import "sync/atomic"

// BucketStatus 代表散列桶状态的类型。
type BucketStatus uint8

const (
	BUCKET_STATUS_NORMAL      BucketStatus = 0
	BUCKET_STATUS_UNDERWEIGHT BucketStatus = 1
	BUCKET_STATUS_OVERWEIGHT  BucketStatus = 2
)

// PairRedistributor 代表针对键-元素对的再分布器。
// 用于当散列段内的键-元素对分布不均时进行重新分布。
type PairRedistributor interface {
	//  UpdateThreshold 会根据键-元素对总数和散列桶总数计算并更新阈值。
	UpdateThreshold(pairTotal uint64, bucketNumber int)
	// CheckBucketStatus 用于检查散列桶的状态。
	CheckBucketStatus(pairTotal uint64, bucketSize uint64) (bucketStatus BucketStatus)
	// Redistribe 用于实施键-元素对的再分布。
	Redistribe(bucketStatus BucketStatus, buckets []Bucket) (newBuckets []Bucket, changed bool)
}

// myPairRedistributor 代表PairRedistributor的默认实现类型。
type myPairRedistributor struct {
	loadFactor            float64
	upperThreshold        uint64
	overweightBucketCount uint64
	emptyBucketCount      uint64
}

// newDefaultPairRedistributor 会创建一个PairRedistributor类型的实例。
func newDefaultPairRedistributor(loadFactor float64, bucketNumber int) PairRedistributor {
	if loadFactor <= 0 {
		loadFactor = DEFAULT_BUCKET_LOAD_FACTOR
	}
	pr := &myPairRedistributor{}
	pr.loadFactor = loadFactor
	pr.UpdateThreshold(0, bucketNumber)
	return pr
}

// bucketCountTemplate 代表调试用散列桶状态信息模板。
var bucketCountTemplate = `Bucket count: 
    pairTotal: %d
    bucketNumber: %d
    average: %f
    upperThreshold: %d
    emptyBucketCount: %d

`

func (pr *myPairRedistributor) UpdateThreshold(pairTotal uint64, bucketNumber int) {
	var average float64
	average = float64(pairTotal / uint64(bucketNumber))
	if average < 100 {
		average = 100
	}
	atomic.StoreUint64(&pr.upperThreshold, uint64(average*pr.loadFactor))
}

// bucketStatusTemplate 代表调试用散列桶状态信息模板。
var bucketStatusTemplate = `Check bucket status: 
    pairTotal: %d
    bucketSize: %d
    upperThreshold: %d
    overweightBucketCount: %d
    emptyBucketCount: %d
    bucketStatus: %d
	
`

func (pr *myPairRedistributor) CheckBucketStatus(pairTotal uint64, bucketSize uint64) (bucketStatus BucketStatus) {
	if bucketSize > DEFAULT_BUCKET_MAX_SIZE ||
		bucketSize >= atomic.LoadUint64(&pr.upperThreshold) {
		atomic.AddUint64(&pr.overweightBucketCount, 1)
		bucketStatus = BUCKET_STATUS_OVERWEIGHT
		return
	}
	if bucketSize == 0 {
		atomic.AddUint64(&pr.emptyBucketCount, 1)
	}
	return
}

// redistributionTemplate 代表重新分配信息模板。
var redistributionTemplate = `Redistributing: 
    bucketStatus: %d
    currentNumber: %d
    newNumber: %d

`

func (pr *myPairRedistributor) Redistribe(
	bucketStatus BucketStatus, buckets []Bucket) (newBuckets []Bucket, changed bool) {
	currentNumber := uint64(len(buckets))
	newNumber := currentNumber
	switch bucketStatus {
	case BUCKET_STATUS_OVERWEIGHT:
		if atomic.LoadUint64(&pr.overweightBucketCount)*4 < currentNumber {
			return nil, false
		}
		newNumber = currentNumber << 1
	case BUCKET_STATUS_UNDERWEIGHT:
		if currentNumber < 100 ||
			atomic.LoadUint64(&pr.emptyBucketCount)*4 < currentNumber {
			return nil, false
		}
		newNumber = currentNumber >> 1
		if newNumber < 2 {
			newNumber = 2
		}
	default:
		return nil, false
	}
	if newNumber == currentNumber {
		atomic.StoreUint64(&pr.overweightBucketCount, 0)
		atomic.StoreUint64(&pr.emptyBucketCount, 0)
		return nil, false
	}
	var pairs []Pair
	for _, b := range buckets {
		for e := b.GetFirstPair(); e != nil; e = e.Next() {
			pairs = append(pairs, e)
		}
	}
	if newNumber > currentNumber {
		for i := uint64(0); i < currentNumber; i++ {
			buckets[i].Clear(nil)
		}
		for j := newNumber - currentNumber; j > 0; j-- {
			buckets = append(buckets, newBucket())
		}
	} else {
		buckets = make([]Bucket, newNumber)
		for i := uint64(0); i < newNumber; i++ {
			buckets[i] = newBucket()
		}
	}
	var count int
	for _, p := range pairs {
		index := int(p.Hash() % newNumber)
		b := buckets[index]
		b.Put(p, nil)
		count++
	}
	atomic.StoreUint64(&pr.overweightBucketCount, 0)
	atomic.StoreUint64(&pr.emptyBucketCount, 0)
	return buckets, true
}
