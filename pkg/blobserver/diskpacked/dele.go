/*
Copyright 2013 The Camlistore Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package diskpacked

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"sync"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/sorted"
	"camlistore.org/pkg/sorted/kvfile"
)

const punchHoleThreshold = 1 << 20

var errNoPunch = errors.New("punchHole not supported")

// for sorting we need BigEndian
var bin = binary.BigEndian

// punchHole, if non-nil, punches a hole in f from offset to offset+size.
var punchHole func(file *os.File, offset int64, size int64) error

func (s *storage) delete(br blob.Ref) error {
	meta, err := s.meta(br)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(s.filename(meta.file), os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer f.Close()

	// walk back, find the header, and overwrite the hash with xxxx-000000...
	b, err := findHeaderBack(f, len(br.String()), meta.offset, meta.size)
	if err != nil {
		return fmt.Errorf("delete: %v", err)
	}
	headerLength := uint16(len(b))
	b = b[1 : len(b)-1] // "sha1-xxxxxxxxxxxxxxxxxx nnnn" - everything between []

	// Replace b with "xxxx-000000000"
	dash := bytes.IndexByte(b, '-')
	if dash < 0 {
		return fmt.Errorf("delete: cannot find dash in ref %q", b)
	}
	space := bytes.IndexByte(b[dash+1:], ' ')
	if space < 0 {
		return fmt.Errorf("delete: cannot find space in header %q", b)
	}
	for i := 0; i < dash; i++ {
		b[i] = 'x'
	}
	for i := dash + 1; i < dash+1+space; i++ {
		b[i] = '0'
	}

	// write back
	if _, err = f.WriteAt(b, meta.offset-int64(headerLength)+1); err != nil {
		return err
	}

	// punch hole, if possible
	if punchHole != nil && meta.size > punchHoleThreshold {
		err = punchHole(f, meta.offset, int64(meta.size))
		if err == nil {
			return nil
		}
		if err != errNoPunch {
			return err
		}
		// err == errNoPunch - not implemented
	}

	// fill with zero
	n, err := f.Seek(meta.offset, os.SEEK_SET)
	if err != nil {
		return err
	}
	if n != meta.offset {
		return fmt.Errorf("error seeking to %d: got %d", meta.offset, n)
	}
	if _, err = io.CopyN(f, zeroReader{}, int64(meta.size)); err != nil {
		return err
	}

	return s.holes.Add(hole{blobMeta: meta, headerLen: headerLength})
}

// findHeaderBack reads the header and returns it.
// Offset is where the blob data starts, and size is its size.
// The header precedes this offset.
func findHeaderBack(f io.ReaderAt, refLen int, offset int64, size uint32) ([]byte, error) {
	// walk back, find the header, and overwrite the hash with xxxx-000000...
	k := 1 + refLen + 1 + len(strconv.FormatUint(uint64(size), 10)) + 1
	off := offset - int64(k)
	b := make([]byte, k)
	var err error
	if k, err = f.ReadAt(b, off); err != nil {
		return nil, err
	}
	if b[0] != byte('[') || b[k-1] != byte(']') {
		return nil, fmt.Errorf("cannot find header surroundings, found %q (offset=%d refLen=%d size=%d)", b, offset, refLen, size)
	}
	return b[:k], nil
}

type zeroReader struct{}

func (z zeroReader) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

// holeList contains the holes in all pack files
type holeList struct {
	sorted.KeyValue
	sync.Mutex // TODO(tgulacsi): maybe not needed - check with -race ?
}

func newHoleList(filename string) (*holeList, error) {
	kv, err := kvfile.NewStorage(filename)
	if err != nil {
		return nil, err
	}
	return &holeList{KeyValue: kv}, nil
}

type hole struct {
	blobMeta
	headerLen uint16
}

func (h hole) String() string {
	return fmt.Sprintf("%d@%d:%d", h.file, h.offset, uint32(h.headerLen)+h.size)
}

func (h hole) MarshalSize(key, value []byte) ([]byte, []byte) {
	if cap(key) < 1+4+8+8 {
		key = make([]byte, 1+4+8+8)
	} else {
		key = key[:1+4+8+8]
	}
	key[0] = 'S'
	bin.PutUint32(key[1:5], h.size)
	bin.PutUint64(key[5:13], uint64(h.file))
	bin.PutUint64(key[13:], uint64(h.offset))

	if cap(value) < 2 {
		value = make([]byte, 2)
	} else {
		value = value[:2]
	}
	bin.PutUint16(value, h.headerLen)
	return key, value
}
func UnmarshalSize(key, value []byte) (hole, error) {
	if len(key) != 21 && key[0] != 'S' && len(value) != 2 {
		if len(key) < 2 {
			return hole{}, fmt.Errorf("bad format value %v/%v", key, value)
		}
		return hole{}, fmt.Errorf("bad format key/value %c%v/%v (len=(%d/%d))", key[0], key[1:], value, len(key), len(value))
	}
	return hole{
		blobMeta: blobMeta{size: bin.Uint32(key[1:5]),
			file:   int(bin.Uint64(key[5:13])),
			offset: int64(bin.Uint64(key[13:]))},
		headerLen: bin.Uint16(value),
	}, nil
}

func (h hole) MarshalBoundary(key, value []byte) ([]byte, []byte) {
	if cap(key) < 1+8+8 {
		key = make([]byte, 1+8+8)
	} else {
		key = key[:1+8+8]
	}
	key[0] = 'B'
	bin.PutUint64(key[1:9], uint64(h.file))
	bin.PutUint64(key[9:], uint64(h.offset))
	if cap(value) < 4+2 {
		value = make([]byte, 4+2)
	} else {
		value = value[:4+2]
	}
	bin.PutUint32(value[:4], h.size)
	bin.PutUint16(value[4:], h.headerLen)
	return key, value
}

func UnmarshalBoundary(key, value []byte) (hole, error) {
	if len(key) != 17 && key[0] != 'B' && len(value) != 6 {
		if len(key) < 2 {
			return hole{}, fmt.Errorf("bad format value %v/%v", key, value)
		}
		return hole{}, fmt.Errorf("bad format key/value %c%v/%v (len=(%d/%d))", key[0], key[1:], value, len(key), len(value))
	}
	return hole{
		blobMeta: blobMeta{size: bin.Uint32(value[:4]),
			file:   int(bin.Uint64(key[1:9])),
			offset: int64(bin.Uint64(key[9:]))},
		headerLen: bin.Uint16(value[4:]),
	}, nil
}

type backIterator interface {
	sorted.Iterator
	Prev() bool
}

// Add adds a new hole into the free list and merges with previous/next hole.
func (hl *holeList) Add(h hole) error {
	var keyA [1 + 8 + 8]byte
	var valA [6]byte
	key, _ := h.MarshalBoundary(keyA[:], valA[:])
	hl.Lock()
	it := hl.Find(string(key[:]), "")

	//FIXME(tgulacsi): proper headerLength (with real size serialization)

	// search for following hole
	if it.Next() && it.KeyBytes()[0] == 'B' {
		next, err := UnmarshalBoundary(it.KeyBytes(), it.ValueBytes())
		if err != nil {
			hl.Unlock()
			return err
		}
		if next.file == h.file && next.offset == h.offset+int64(h.headerLen)+int64(h.size) {
			old := h
			h.size += uint32(next.headerLen) + next.size
			log.Printf("Hole merging forwards %s with %s = %s", old, next, h)
			hl.Delete(it.Key())
		}
	}
	it.Close()

	// search for previous hole - walk from the beginning, as Iterator has no Prev()
	h2 := h
	h2.offset = 0
	key, _ = h2.MarshalBoundary(keyA[:], valA[:])
	it = hl.Find(string(key[:]), "")
	for it.Next() && it.KeyBytes()[0] == 'B' {
		prev, err := UnmarshalBoundary(it.KeyBytes(), it.ValueBytes())
		if err != nil {
			hl.Unlock()
			return err
		}
		if prev.file != h.file || prev.offset >= h.offset {
			break
		}
		if prev.offset+int64(prev.headerLen)+int64(prev.size) == h.offset {
			old := h
			h.offset = prev.offset
			h.size += uint32(prev.headerLen) + prev.size
			log.Printf("Hole merging backwards %s with %s = %s", old, prev, h)
			break
		}
	}
	it.Close()

	// size list
	key, value := h.MarshalSize(nil, make([]byte, 6))
	err := hl.Set(string(key), string(value))
	if err == nil {
		// boundaries
		key, value = h.MarshalBoundary(key, value)
		err = hl.Set(string(key), string(value))
	}
	hl.Unlock()
	return err
}

// ErrNoHole signs that no big enough hole found
var ErrNoHole = errors.New("no fitting hole found")

// Acquire searches for a big enough hole, calls the given function with the hole's
// data and deletes the hole if the function returns successfully - otherwise the
// hole remains and the error is returned.
// If no hole found, ErrNoHole is returned.
func (hl *holeList) Acquire(size uint32, todo func(hole) error) error {
	var keyA [1 + 4 + 8 + 8]byte
	keyA[0] = 'S'
	bin.PutUint32(keyA[1:5], size)
	found := false
	hl.Lock()
	it := hl.Find(string(keyA[:]), "")
	var (
		key, value []byte
		h          hole
		err        error
	)
	for it.Next() {
		key, value = it.KeyBytes(), it.ValueBytes()
		if key[0] != 'S' {
			break
		}
		// This size check is just for being sure
		h, err = UnmarshalSize(key, value)
		if err != nil {
			continue
		}
		if h.size >= size {
			found = true
			if err = hl.Delete(it.Key()); err != nil {
				hl.Unlock()
				return err
			}
			break
		}
	}
	hl.Unlock()
	if err = it.Close(); err != nil {
		return err
	}
	if !found {
		return ErrNoHole
	}
	if err != nil {
		return err
	}
	// Shall not hold the lock when calling todo.
	if err := todo(h); err != nil {
		// put hole back
		hl.Lock()
		if e := hl.Set(string(key), string(value)); e != nil {
			log.Printf("cannot put hole back: %v", e)
		}
		hl.Unlock()
		return err
	}
	return nil
}
