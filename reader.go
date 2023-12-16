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
	n, err = file.Read(into)
	if err == io.EOF && r.fno+1 < len(r.log.fds) {
		r.fno++
		r.log.fds[r.fno].Seek(0, 0) // FIXME
		return r.Read(into)
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
	start := r.log.offsets[k]
	p, err := r.log.fds[k].Seek(offset-start, 0)
	if err == nil {
		p += start
	}
	return p, err
}
