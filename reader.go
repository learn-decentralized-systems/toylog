package toylog

import (
	"errors"
	"golang.org/x/sys/unix"
	"io"
)

type ChunkedLogReader struct {
	log *ChunkedLog
	pos int64
	off int64
	fd  int
}

func (log *ChunkedLog) Reader(pos int64, whence int) (*ChunkedLogReader, error) {
	reader := ChunkedLogReader{log: log}
	_, err := reader.Seek(pos, whence)
	return &reader, err
}

func (r *ChunkedLogReader) Read(into []byte) (n int, err error) {
	chunk_off := r.pos - r.off
	n, err = unix.Pread(r.fd, into, chunk_off)
	if err != nil {
		return
	}
	if n == 0 { // EOF
		old_off := r.off                     // no 0 length chunks
		_, err = r.Seek(r.pos, io.SeekStart) // afresh
		if err != nil || old_off == r.off {
			return
		}
		return r.Read(into)
	}
	r.pos += int64(n)
	return
}

var ErrChunkMissing = errors.New("the requested chunk is missing")
var ErrOutOfRange = errors.New("the requested offset is out of range")

func (r *ChunkedLogReader) Seek(offset int64, whence int) (int64, error) {
	fd, off, fpos, err := r.log.Locate(offset, whence)
	if err != nil {
		return r.pos, err
	}
	if r.fd != 0 {
		_ = unix.Close(r.fd)
	}
	r.fd = fd
	r.off = off
	r.pos = off + int64(fpos)
	return r.pos, nil
}

func (r *ChunkedLogReader) Close() (err error) {
	if r.fd == 0 {
		return
	}
	err = unix.Close(r.fd)
	r.fd = 0
	r.log = nil
	r.off = 0
	r.pos = 0
	return
}
