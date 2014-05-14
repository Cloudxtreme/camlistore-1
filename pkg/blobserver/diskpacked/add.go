/*
Copyright 2014 The Camlistore Authors.

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
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/sorted"
)

const (
	maxBlobSize10 = 10 // how many digits needed for printing constants.MaxBlobSize?
	// FIXME(tgulacsi): do we need this length of invalid ref?
	invalidRef    = "xxxx-0000000000000000000000000000000000000000"
	invalidRefLen = len(invalidRef)
)

var restRefHeader = fmt.Sprintf("[%s %%0%dd]", invalidRef, maxBlobSize10)

// append writes the provided blob to the current data file.
func (s *storage) append(br blob.SizedRef, r io.Reader) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("diskpacked: write to closed storage")
	}

	// try to find a hole
	header := fmt.Sprintf("[%v %v]", br.Ref.String(), br.Size)
	err := s.holes.Acquire(
		br.Size+uint32(len(header)),
		func(h hole) error {
			if br.Size > h.size {
				return fmt.Errorf("hole too tight (want %d, got %d)", br.Size, h.size)
			}
			fn := s.filename(h.file)
			f, err := os.OpenFile(fn, os.O_RDWR, 0666)
			if err != nil {
				return err
			}
			defer f.Close()
			addWritten := func(n int64) {
				writeVar.Add(fn, n)
				writeTotVar.Add(s.root, n)
			}
			data := blobMeta{file: h.file}
			rest := hole{blobMeta: data}

			off, err := f.Seek(h.offset-int64(h.headerLen), 0)
			if err != nil {
				return err
			}
			n1, err := io.WriteString(f, header)
			if err != nil {
				return err
			}
			data.offset = off + int64(n1)
			addWritten(int64(n1))

			n2, err := io.Copy(f, r)
			if err != nil {
				return err
			}
			data.size = uint32(n2)
			off = data.offset + n2
			addWritten(n2)

			// write a dummy header for the remaining hole
			// net size + (preceding) header - written header length - written data length
			remaining := int64(h.size) + int64(h.headerLen) - int64(n1) - n2
			// - this headeruint32's length
			holeHeaderSize := 1 + len(invalidRef) + 1 + maxBlobSize10 + 1
			remaining -= int64(holeHeaderSize)
			if remaining < 0 {
				return fmt.Errorf(
					"hole too tight (want %d, got %d (hole size=%d, written=%d holeHeader=%d => remains %d))",
					br.Size, h.size, int64(h.size)+int64(h.headerLen),
					int64(n1)+n2, holeHeaderSize, int64(remaining))
			}

			if n1, err = fmt.Fprintf(f, restRefHeader, rest); err != nil {
				return err
			}
			h.offset = off + int64(n1)
			addWritten(int64(n1))
			if err = f.Sync(); err != nil {
				return err
			}
			// insert data position into the index
			if err = s.index.Set(br.Ref.String(), data.String()); err != nil {
				return err
			}
			// insert the new (smaller) hole's data into the free list
			rest.offset = data.offset + int64(data.size) + int64(n1)
			//h.offset + h.size = data.offset + data.size + n1 + hole.size
			//h.offset + h.size = off + hole.size
			rest.size = uint32((h.offset + int64(h.size)) - rest.offset)
			if e := s.holes.Add(rest); e != nil {
				log.Printf("hole %#v headerLength %d: %v", rest, n1, e)
			}
			return nil
		})
	if err == nil { // successfully filled a hole
		return nil
	}
	if err != ErrNoHole {
		// FIXME(tgulacsi): error out, or just log and append?
		log.Printf("error writing %s into a hole: %v", br.Ref, err)
	}

	fn := s.writer.Name()
	addWritten := func(n int64) {
		writeVar.Add(fn, n)
		writeTotVar.Add(s.root, n)
	}
	n, err := fmt.Fprintf(s.writer, "[%v %v]", br.Ref.String(), br.Size)
	s.size += int64(n)
	addWritten(int64(n))
	if err != nil {
		return err
	}

	// TODO(adg): remove this seek and the offset check once confident
	offset, err := s.writer.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}
	if offset != s.size {
		return fmt.Errorf("diskpacked: seek says offset = %d, we think %d",
			offset, s.size)
	}
	offset = s.size // make this a declaration once the above is removed

	n2, err := io.Copy(s.writer, r)
	s.size += n2
	addWritten(int64(n))
	if err != nil {
		return err
	}
	if n2 != int64(br.Size) {
		return fmt.Errorf("diskpacked: written blob size %d didn't match size %d", n, br.Size)
	}
	if err = s.writer.Sync(); err != nil {
		return err
	}

	packIdx := len(s.fds) - 1
	if s.size > s.maxFileSize {
		if err := s.nextPack(); err != nil {
			return err
		}
	}
	return s.index.Set(br.Ref.String(), blobMeta{packIdx, offset, br.Size}.String())
}

// meta fetches the metadata for the specified blob from the index.
func (s *storage) meta(br blob.Ref) (m blobMeta, err error) {
	ms, err := s.index.Get(br.String())
	if err != nil {
		if err == sorted.ErrNotFound {
			err = os.ErrNotExist
		}
		return
	}
	m, ok := parseBlobMeta(ms)
	if !ok {
		err = fmt.Errorf("diskpacked: bad blob metadata: %q", ms)
	}
	return
}

// blobMeta is the blob metadata stored in the index.
type blobMeta struct {
	file   int
	offset int64
	size   uint32
}

func parseBlobMeta(s string) (m blobMeta, ok bool) {
	n, err := fmt.Sscan(s, &m.file, &m.offset, &m.size)
	return m, n == 3 && err == nil
}

func (m blobMeta) String() string {
	return fmt.Sprintf("%v %v %v", m.file, m.offset, m.size)
}

func (m blobMeta) SizedRef(br blob.Ref) blob.SizedRef {
	return blob.SizedRef{Ref: br, Size: m.size}
}
