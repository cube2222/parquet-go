package bloom

import (
	"io"
	"sync"
	"unsafe"

	"github.com/segmentio/parquet-go/internal/bits"
)

// Filter is an interface representing read-only bloom filters where programs
// can probe for the possible presence of a hash key.
type Filter interface {
	Check(uint64) bool
}

// MutableFilter is an extension of the Filter interface which supports
// inserting keys to the filter.
type MutableFilter interface {
	Filter
	Reset()
	Insert(uint64)
	InsertBulk([]uint64)
	Bytes() []byte
}

// SplitBlockFilter is an in-memory implementation of the parquet bloom filters.
//
// This type is useful to construct bloom filters that are later serialized
// to a storage medium.
type SplitBlockFilter []Block

// MakeSplitBlockFilter constructs a SplitBlockFilter value from the data byte
// slice.
func MakeSplitBlockFilter(data []byte) SplitBlockFilter {
	p := *(*unsafe.Pointer)(unsafe.Pointer(&data))
	n := len(data) / BlockSize
	return unsafe.Slice((*Block)(p), n)
}

// NumSplitBlocksOf returns the number of blocks in a filter intended to hold
// the given number of values and bits of filter per value.
//
// This function is useful to determine the number of blocks when creating bloom
// filters in memory, for example:
//
//	f := make(bloom.SplitBlockFilter, bloom.NumSplitBlocksOf(n, 10))
//
func NumSplitBlocksOf(numValues int64, bitsPerValue uint) int {
	numBytes := bits.ByteCount(uint(numValues) * bitsPerValue)
	numBlocks := (numBytes + (BlockSize - 1)) / BlockSize
	return numBlocks
}

// Reset clears the content of the filter f.
func (f SplitBlockFilter) Reset() {
	for i := range f {
		f[i] = Block{}
	}
}

// Block returns a pointer to the block that the given value hashes to in the
// bloom filter.
func (f SplitBlockFilter) Block(x uint64) *Block { return &f[fasthash1x64(x, int32(len(f)))] }

// InsertBulk adds all values from x into f.
func (f SplitBlockFilter) InsertBulk(x []uint64) { filterInsertBulk(f, x) }

// Insert adds x to f.
func (f SplitBlockFilter) Insert(x uint64) { filterInsert(f, x) }

// Check tests whether x is in f.
func (f SplitBlockFilter) Check(x uint64) bool { return filterCheck(f, x) }

// Bytes converts f to a byte slice.
//
// The returned slice shares the memory of f. The method is intended to be used
// to serialize the bloom filter to a storage medium.
func (f SplitBlockFilter) Bytes() []byte {
	return unsafe.Slice(*(**byte)(unsafe.Pointer(&f)), len(f)*BlockSize)
}

// CheckSplitBlock is similar to bloom.SplitBlockFilter.Check but reads the
// bloom filter of n bytes from r.
//
// The size n of the bloom filter is assumed to be a multiple of the block size.
func CheckSplitBlock(r io.ReaderAt, n int64, x uint64) (bool, error) {
	block := acquireBlock()
	defer releaseBlock(block)
	offset := BlockSize * fasthash1x64(x, int32(n/BlockSize))
	_, err := r.ReadAt(block.Bytes(), int64(offset))
	return block.Check(uint32(x)), err
}

var (
	_ MutableFilter = (SplitBlockFilter)(nil)

	blockPool sync.Pool
)

func acquireBlock() *Block {
	b, _ := blockPool.Get().(*Block)
	if b == nil {
		b = new(Block)
	}
	return b
}

func releaseBlock(b *Block) {
	if b != nil {
		blockPool.Put(b)
	}
}
