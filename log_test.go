package toylog

import (
	"encoding/binary"
	"github.com/learn-decentralized-systems/toyqueue"
	"github.com/stretchr/testify/assert"
	"io"
	"os"
	"sync"
	"testing"
)

func TestRecords_Prefix(t *testing.T) {
	recs := toyqueue.Records{{1}, {1, 2}, {1, 2, 3}, {1, 2, 3, 4}}
	assert.Equal(t, int64(10), recs.TotalLen())
	suff := recs.ExactSuffix(5)
	assert.Equal(t, int64(5), suff.TotalLen())
	assert.Equal(t, recs[2][2:], suff[0])
	pref, rem := recs.WholeRecordPrefix(5)
	assert.Equal(t, int64(3), pref.TotalLen())
	assert.Equal(t, int64(2), rem)
}

func TestChunkedLog_Open(t *testing.T) {
	const N = 1 << 17
	log := ChunkedLog{
		MaxChunkSize: 1 << 15,
		MaxChunks:    1 << 5,
		Synced:       false,
	}
	os.RemoveAll("chunked.log")
	err := log.Open("chunked.log")
	assert.Nil(t, err)
	for i := uint64(0); i < N; i++ {
		var b [8]byte
		binary.LittleEndian.PutUint64(b[:], i)
		n, err := log.Write(b[:])
		assert.Equal(t, 8, n)
		assert.Nil(t, err)
	}

	reader, err := log.Reader(0, io.SeekStart)
	assert.Nil(t, err)
	for i := uint64(0); i < N; i++ {
		var b [8]byte
		n, err := reader.Read(b[:])
		assert.Nil(t, err)
		assert.Equal(t, 8, n)
		j := binary.LittleEndian.Uint64(b[:])
		assert.Equal(t, i, j)
	}

	err = log.Close()
	assert.Nil(t, err)
	os.RemoveAll("chunked.log")
}

func TestChunkedLog_Write(t *testing.T) {
	const N = 1 << 8 // 8K
	const K = 1 << 2 // 16
	log := ChunkedLog{
		MaxChunkSize: 1 << 10,  // 4K
		MaxChunks:    (1 << 3), // 32
		Synced:       true,
	}
	os.RemoveAll("concurrent.log")
	err := log.Open("concurrent.log")
	assert.Nil(t, err)
	lock := sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(K)
	for k := 0; k < K; k++ {
		go func(k int) {
			i := uint64(k) << 32
			for n := uint64(0); n < N; n++ {
				var b [8]byte
				binary.LittleEndian.PutUint64(b[:], i|n)
				lock.Lock()
				n, err := log.Write(b[:])
				lock.Unlock()
				assert.Equal(t, 8, n)
				assert.Nil(t, err)
			}
			wg.Done()
		}(k)
	}
	wg.Wait()

	reader, err := log.Reader(0, io.SeekStart)
	assert.Nil(t, err)
	check := [K]int{}
	for i := uint64(0); i < N*K; i++ {
		var b [8]byte
		r, err := reader.Read(b[:])
		assert.Nil(t, err)
		assert.Equal(t, 8, r)
		j := binary.LittleEndian.Uint64(b[:])
		k := int(j >> 32)
		n := int(j & 0xffffffff)
		assert.Equal(t, check[k], n)
		check[k] = n + 1
	}

	err = log.Close()
	assert.Nil(t, err)
	_ = os.RemoveAll("concurrent.log")
}
