package toylog

import (
	"errors"
	"golang.org/x/sys/unix"
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
	n, err = unix.Pread(file, into, chunk_off)
	if n == 0 { // EOF
		if r.fno+1 < len(r.log.fds) {
			r.fno++
			return r.Read(into)
		}
	}
	r.pos += int64(n)
	return
}

var ErrChunkMissing = errors.New("the requested chunk is missing")
var ErrOutOfRange = errors.New("the requested offset is out of range")

func (r *ChunkedLogReader) Seek(offset int64, whence int) (int64, error) {
	if offset < r.log.offsets[0] {
		return -1, ErrChunkMissing
	}
	if offset > r.log.wrlen {
		return -1, ErrOutOfRange
	}
	l := len(r.log.offsets)
	k := 0
	for k+1 < l {
		if offset < r.log.offsets[k+1] {
			break
		}
		k++
	}
	r.fno = k
	r.pos = offset
	return offset, nil
}
