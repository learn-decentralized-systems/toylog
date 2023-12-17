package toylog

import (
	"errors"
	"io"
)

type ChunkedLogReader struct {
	log *ChunkedLog
	pos int64
	fno int
}

func (log *ChunkedLog) Reader(pos int64) (*ChunkedLogReader, error) {
	reader := ChunkedLogReader{log: log}
	_, err := reader.Seek(pos, 0)
	return &reader, err
}

func (r *ChunkedLogReader) Read(into []byte) (n int, err error) {
	file := r.log.fds[r.fno]
	chunk_off := r.pos - r.log.offsets[r.fno]
	n, err = file.ReadAt(into, chunk_off)
	r.pos += int64(n)
	if r.pos == r.log.offsets[r.fno+1] && r.fno+1 < len(r.log.fds) {
		r.fno++
		if err == io.EOF {
			return r.Read(into)
		}
	}
	return
}

var ErrChunkMissing = errors.New("the requested chunk is missing")
var ErrOutOfRange = errors.New("the requested offset is out of range")

func (r *ChunkedLogReader) Seek(offset int64, whence int) (int64, error) {
	if offset < r.log.offsets[0] {
		return -1, ErrChunkMissing
	}
	l := len(r.log.offsets)
	k := 0
	for k+1 < l {
		f := r.log.offsets[k]
		t := r.log.offsets[k+1]
		if offset >= f && offset < t {
			break
		}
		k++
	}
	if k+1 == l {
		return -1, ErrOutOfRange
	}
	r.fno = k
	r.pos = offset
	return offset, nil
}
