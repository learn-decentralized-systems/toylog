package toylog

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func PumpInts(log *ChunkedLog, fro, till uint32, t *testing.T) {
	for i := fro; i < till; i++ {
		var b [4]byte
		binary.LittleEndian.PutUint32(b[:], i)
		n, err := log.Write(b[:])
		assert.Nil(t, err)
		assert.Equal(t, 4, n)
	}
}

func TestChunkedLog_Write(t *testing.T) {
	const M = 1 << 20
	log := ChunkedLog{
		MaxChunkSize: M,
		MaxChunks:    4,
	}
	_ = os.RemoveAll("log")
	err := log.Open("log")
	if err != nil {
		t.Fatal(err.Error())
	}

	PumpInts(&log, 0, M, t)
	assert.Equal(t, 4, log.ChunkCount())
	assert.Equal(t, int64(4*M), log.TotalSize())
	assert.Equal(t, int64(4*M), log.CurrentSize())
	assert.Equal(t, int64(0), log.ExpiredSize())

	PumpInts(&log, 0, M, t)
	assert.Equal(t, 4, log.ChunkCount())
	assert.Equal(t, int64(8*M), log.TotalSize())
	assert.Equal(t, int64(4*M), log.CurrentSize())
	assert.Equal(t, int64(4*M), log.ExpiredSize())

	_ = os.RemoveAll("log")
}

func TestLogReader_Seek(t *testing.T) {
	// TODO prepare 4M
	const M = uint32(1 << 10)
	log := ChunkedLog{
		MaxChunkSize: int64(M),
		MaxChunks:    4,
	}
	_ = os.RemoveAll("log")
	err := log.Open("log")
	if err != nil {
		t.Fatal(err.Error())
	}

	PumpInts(&log, 0, 2*M, t)
	assert.Equal(t, 4, log.ChunkCount())
	assert.Equal(t, int64(8*M), log.TotalSize())
	assert.Equal(t, int64(4*M), log.CurrentSize())
	assert.Equal(t, int64(4*M), log.ExpiredSize())

	reader, err := log.Reader(int64(M * 4))
	assert.Nil(t, err)
	for i := M; i < 2*M; i++ {
		var buf [4]byte
		n, err := reader.Read(buf[:])
		assert.Nil(t, err)
		assert.Equal(t, 4, n)
		rec := binary.LittleEndian.Uint32(buf[:])
		assert.Equal(t, i, rec)
	}
}
