package toylog

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"
)

type ChunkedLog struct {
	offsets      []int64
	fds          []*os.File
	MaxChunkSize int64
	MaxChunks    int
	dir          string
}

var ErrNotOpen = errors.New("the log is not open")
var ErrAlreadyOpen = errors.New("the log is already open")
var ErrOmission = errors.New("the log has missing chunks")
var ErrOverlap = errors.New("the log has overlapping chunks")

func (log *ChunkedLog) Open(dir string) (err error) {
	if len(log.offsets) > 0 {
		return ErrAlreadyOpen
	}
	info, err := os.Stat(dir)
	if err != nil {
		err = os.MkdirAll(dir, os.ModeDir|os.ModePerm)
		if err != nil {
			return err
		}
	} else if !info.IsDir() {
		return fs.ErrExist
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	filenames := []string{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".log") || len(e.Name()) != 12+4 {
			continue
		}
		filenames = append(filenames, e.Name())
	}
	log.dir = dir
	if len(filenames) == 0 {
		path := log.fn4pos(0)
		file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
		if err != nil {
			return err
		}
		log.offsets = append(log.offsets, 0, 0)
		log.fds = append(log.fds, file)
		return nil
	}
	next := int64(0)
	sort.Strings(filenames)
	for _, fn := range filenames {
		var start int64
		n, err := fmt.Sscanf(fn, "%d.log", &start)
		if err != nil || n != 1 {
			continue
		}
		if next == 0 {
			next = start
		} else if next < start {
			return ErrOmission
		} else if next > start {
			return ErrOverlap
		}
		file, err := os.Open(fn)
		if err != nil {
			return err
		}
		stat, err := file.Stat()
		if err != nil {
			return err
		}
		next += stat.Size()
		log.offsets = append(log.offsets, start)
		log.fds = append(log.fds, file)
	}
	log.offsets = append(log.offsets, next)
	return nil
}

func (log *ChunkedLog) lastChunkSize() int64 {
	l := len(log.offsets)
	return log.offsets[l-1] - log.offsets[l-2]
}

func (log *ChunkedLog) fn4pos(pos int64) string {
	return fmt.Sprintf(log.dir+string(os.PathSeparator)+"%012d.log", pos)
}

func (log *ChunkedLog) expireChunk() {
	pos0 := log.offsets[0]
	path0 := log.fn4pos(pos0)
	_ = log.fds[0].Close()
	_ = os.Remove(path0)
	log.fds = log.fds[1:]
	log.offsets = log.offsets[1:]
}

func (log *ChunkedLog) RotateChunks() error {
	pos := log.TotalSize()
	path := log.fn4pos(pos)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		return err
	}
	log.fds = append(log.fds, file)
	log.offsets = append(log.offsets, log.offsets[len(log.offsets)-1])
	for log.ChunkCount() > log.MaxChunks && len(log.fds) > 1 {
		log.expireChunk()
	}
	return nil
}

func (log *ChunkedLog) Write(p []byte) (n int, err error) {
	if len(log.fds) == 0 {
		return 0, ErrNotOpen
	}
	if log.lastChunkSize()+int64(len(p)) > log.MaxChunkSize {
		err = log.RotateChunks()
		if err != nil {
			return
		}
	}
	last := log.fds[len(log.fds)-1]
	for len(p) > 0 {
		k, err := last.Write(p)
		n += k
		if err != nil {
			return n, err
		}
		p = p[k:]
	}
	log.offsets[len(log.offsets)-1] += int64(n)
	return
}

func (log *ChunkedLog) Sync() error {
	l := len(log.fds)
	if l == 0 {
		return ErrNotOpen
	}
	return log.fds[l-1].Sync()
}

func (log *ChunkedLog) Close() {
	for _, fd := range log.fds {
		_ = fd.Close()
	}
	log.offsets = log.offsets[:0]
	log.fds = log.fds[:0]
}

func (log *ChunkedLog) ChunkCount() int {
	return len(log.fds)
}

func (log *ChunkedLog) TotalSize() int64 {
	return log.offsets[len(log.offsets)-1]
}

func (log *ChunkedLog) ExpiredSize() int64 {
	return log.offsets[0]
}

func (log *ChunkedLog) CurrentSize() int64 {
	return log.TotalSize() - log.ExpiredSize()
}
