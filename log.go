package toylog

import (
	"errors"
	"fmt"
	"github.com/learn-decentralized-systems/toyqueue"
	"golang.org/x/sys/unix"
	"io/fs"
	"os"
	"sort"
	"strings"
)

type ChunkedLog struct {
	// Chunk size never exceeds this number (can not make a bigger write)
	MaxChunkSize int64
	// Old chunks above this number are dropped
	MaxChunks int
	// whether all writes are `fsynced` *before* `Write()` returns
	Synced bool
	// header maker for new chunks
	Header toyqueue.Feeder

	dir     string
	offsets []int64
	fds     []int

	// The total queued/written/synced length of the log. "Total" means
	// counting all past chunks, including those already dropped.
	wrlen, sylen int64
	reclen       int
}

var ErrNotOpen = errors.New("the log is not open")
var ErrAlreadyOpen = errors.New("the log is already open")
var ErrOmission = errors.New("the log has missing chunks")
var ErrOverlap = errors.New("the log has overlapping chunks")

const Suffix = ".log.chunk"

func (log *ChunkedLog) Open(dir string) (err error) {
	if len(log.offsets) > 0 {
		return ErrAlreadyOpen
	}
	if log.MaxChunks == 0 {
		log.MaxChunks = 8
	}
	if log.MaxChunkSize == 0 {
		log.MaxChunkSize = 1 << 23
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
		if e.IsDir() || !strings.HasSuffix(e.Name(), Suffix) || len(e.Name()) != 12+4 {
			continue
		}
		filenames = append(filenames, e.Name())
	}
	log.dir = dir
	if len(filenames) == 0 {
		path := log.fn4pos(0)
		file, err := unix.Open(path, unix.O_CREAT|unix.O_APPEND|unix.O_RDWR, 0660)
		if err != nil {
			return err
		}
		log.offsets = append(log.offsets, 0)
		log.fds = append(log.fds, file)
		return nil
	}
	next := int64(0)
	sort.Strings(filenames)
	for _, fn := range filenames {
		var start int64
		n, err := fmt.Sscanf(fn, "%d"+Suffix, &start)
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
		path := dir + string(os.PathSeparator) + fn
		file, err := unix.Open(path, unix.O_RDONLY, 0)
		if err != nil {
			return err
		}
		stat := unix.Stat_t{}
		err = unix.Stat(path, &stat)
		if err != nil {
			return err
		}
		next += stat.Size
		log.offsets = append(log.offsets, start)
		log.fds = append(log.fds, file)
	}
	log.wrlen = next
	log.sylen = next
	return nil
}

func (log *ChunkedLog) lastChunkSize() int64 {
	l := len(log.offsets)
	return log.offsets[l-1] - log.offsets[l-2]
}

func (log *ChunkedLog) fn4pos(pos int64) string {
	return fmt.Sprintf(log.dir+string(os.PathSeparator)+"%012d"+Suffix, pos)
}

func (log *ChunkedLog) expireChunk() {
	pos0 := log.offsets[0]
	path0 := log.fn4pos(pos0)
	_ = unix.Close(log.fds[0])
	_ = os.Remove(path0)
	log.fds = log.fds[1:]
	log.offsets = log.offsets[1:]
}

func (log *ChunkedLog) rotateChunks() error {
	_ = unix.Fsync(log.fds[len(log.fds)-1])
	pos := log.TotalSize()
	path := log.fn4pos(pos)
	file, err := unix.Open(path, unix.O_CREAT|unix.O_APPEND|unix.O_RDWR, 0660)
	if err != nil {
		return err
	}
	log.fds = append(log.fds, file)
	log.offsets = append(log.offsets, log.wrlen)
	for log.ChunkCount() > log.MaxChunks && len(log.fds) > 1 {
		log.expireChunk()
	}
	return nil
}

func (log *ChunkedLog) Write(rec []byte) (n int, err error) {
	recs := [][]byte{rec}
	err = log.Drain(recs)
	if err == nil {
		n = len(rec)
	}
	return
}

// We expect a dense stream of tiny ops here, so we bundle writes here.
// Note: a slice goes into one chunk of the log, no torn writes.
func (log *ChunkedLog) Drain(recs toyqueue.Records) (err error) {
	if len(log.fds) == 0 {
		return ErrNotOpen
	}

	recsToWrite := recs

	for err == nil && len(recsToWrite) > 0 {
		chunkNo := len(log.fds) - 1
		chunkFilled := log.wrlen - log.offsets[chunkNo]
		chunkCap := log.MaxChunkSize - chunkFilled
		if chunkCap < 0 {
			chunkCap = 0
		}
		recsFitChunk, _ := recsToWrite.WholeRecordPrefix(chunkCap)
		if len(recsFitChunk) == 0 {
			err = log.rotateChunks() // fsyncs
			if err != nil {
				break
			}
			if log.Header != nil {
				header, _ := log.Header.Feed()
				recsToWrite = append(header, recsToWrite...)
			}
			continue
		}
		fd := log.fds[chunkNo]

		bytesWritten := 0
		bytesWritten, err = unix.Writev(fd, recsFitChunk)

		if err != nil {
			break
		}
		recsToWrite = recsToWrite.ExactSuffix(int64(bytesWritten))

		log.wrlen += int64(bytesWritten)
	}
	if log.Synced {
		fd := log.fds[len(log.fds)-1]
		err = unix.Fsync(fd)
		log.sylen = log.wrlen
	}

	log.reclen += len(recs)

	return err
}

func (log *ChunkedLog) Sync() error {
	l := len(log.fds)
	if l == 0 {
		return ErrNotOpen
	}
	return unix.Fsync(log.fds[l-1])
}

func (log *ChunkedLog) Close() error {
	if len(log.fds) == 0 {
		return ErrNotOpen
	}
	for _, fd := range log.fds {
		_ = unix.Close(fd)
	}
	log.offsets = log.offsets[:0]
	log.fds = log.fds[:0]
	return nil
}

func (log *ChunkedLog) ChunkCount() int {
	return len(log.fds)
}

func (log *ChunkedLog) TotalSize() int64 {
	return log.wrlen
}

func (log *ChunkedLog) ExpiredSize() int64 {
	return log.offsets[0]
}

func (log *ChunkedLog) CurrentSize() int64 {
	return log.TotalSize() - log.ExpiredSize()
}
