package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

// define the number of bytes that make up each index entry
var (
	offWidth uint64 = 4                   // record offset
	posWidth uint64 = 8                   // position in the store file
	entWidth        = offWidth + posWidth // use to jump to the position of an entry given offset
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

// Creates an index for the given file.
// Saves current size of file. Grows the file to max index size before memory-mapping
// the file and then return te index to caller.
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())
	if err = os.Truncate(f.Name(),
		int64(c.Segment.MaxIndexBytes),
	); err != nil {
		return nil, err
	}
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

// Makes sure the memory-mapped file has synced data to persisted file,
// and file has flushed contents to storage.
// Then truncates to the amount of data actually in it and closes file.
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

// Takes in an offset, returns associated records position in the store.
// Given offset is relative to segment's base offset. 0 is first entry, 1 is second.
// Reduces the size of indexes by storing offsets as uint32s.
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

// Appends the given offset and position to index.
func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth)
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
