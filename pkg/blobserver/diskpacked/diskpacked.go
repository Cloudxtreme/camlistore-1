/*
Copyright 2013 Google Inc.

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

/*
Package diskpacked registers the "diskpacked" blobserver storage type,
storing blobs in sequence of monolithic data files indexed by a kvfile index.

Example low-level config:

     "/storage/": {
         "handler": "storage-diskpacked",
         "handlerArgs": {
            "path": "/var/camlistore/blobs"
          }
     },

*/
package diskpacked

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/blobserver"
	"camlistore.org/pkg/index"
	"camlistore.org/pkg/index/kvfile"
	"camlistore.org/pkg/jsonconfig"
	"camlistore.org/pkg/types"
)

const defaultMaxFileSize = 1 << 29 // 512MB

type storage struct {
	root        string
	index       index.Storage
	indexCloser io.Closer
	maxFileSize int64

	mu       sync.Mutex
	current  *os.File
	currentN int64
	closed   bool
}

func New(root string) (*storage, error) {
	fi, err := os.Stat(root)
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("storage root %q doesn't exist", root)
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to stat directory %q: %v", root, err)
	}
	if !fi.IsDir() {
		return nil, fmt.Errorf("storage root %q exists but is not a directory.", root)
	}
	index, closer, err := kvfile.NewStorage(filepath.Join(root, "index.kv"))
	s := &storage{root: root, index: index, indexCloser: closer, maxFileSize: defaultMaxFileSize}
	if err := s.openCurrent(); err != nil {
		return nil, err
	}
	return s, nil
}

func newFromConfig(_ blobserver.Loader, config jsonconfig.Obj) (storage blobserver.Storage, err error) {
	path := config.RequiredString("path")
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return New(path)
}

func init() {
	blobserver.RegisterStorageConstructor("diskpacked", blobserver.StorageConstructor(newFromConfig))
}

// openCurrent makes sure the current data file is open as s.current.
func (s *storage) openCurrent() error {
	if s.current == nil {
		// First run; find the latest file data file and open it
		// and seek to the end.
		// If no data files exist, leave s.current as nil.
		for {
			_, err := os.Stat(s.filename(s.currentN))
			if os.IsNotExist(err) {
				break
			}
			if err != nil {
				return err
			}
			s.currentN++
		}
		if s.currentN > 0 {
			s.currentN--
			f, err := os.OpenFile(s.filename(s.currentN), os.O_RDWR, 0666)
			if err != nil {
				return err
			}
			if _, err = f.Seek(0, 2); err != nil {
				return err
			}
			s.current = f
		}
	}

	// If s.current is open, check its size and if it's too big close it,
	// and advance currentN.
	if s.current != nil {
		fi, err := s.current.Stat()
		if err != nil {
			return err
		}
		if fi.Size() > s.maxFileSize {
			f := s.current
			s.current = nil
			s.currentN++
			if err = f.Close(); err != nil {
				return err
			}
		}
	}

	// If we don't have the current file open, make one.
	if s.current == nil {
		f, err := os.Create(s.filename(s.currentN))
		if err != nil {
			return err
		}
		s.current = f
	}
	return nil
}

func (s *storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.indexCloser.Close(); err != nil {
		log.Println("diskpacked: closing index:", err)
	}
	s.closed = true
	if s.current != nil {
		return s.current.Close()
	}
	return nil
}

func (s *storage) FetchStreaming(br blob.Ref) (io.ReadCloser, int64, error) {
	return s.Fetch(br)
}

func (s *storage) Fetch(br blob.Ref) (types.ReadSeekCloser, int64, error) {
	meta, err := s.meta(br)
	if err != nil {
		return nil, 0, err
	}
	// TODO(adg): pool open file descriptors
	f, err := os.Open(s.filename(meta.file))
	if err != nil {
		return nil, 0, err
	}
	return openFile{io.NewSectionReader(f, meta.offset, meta.size), f}, meta.size, nil
}

type openFile struct {
	io.ReadSeeker
	io.Closer
}

func (s *storage) filename(file int64) string {
	return filepath.Join(s.root, fmt.Sprintf("data-%v", file))
}

func (s *storage) RemoveBlobs(blobs []blob.Ref) error {
	// TODO(adg): remove blob from index and pad data with spaces
	return errors.New("diskpacked: RemoveBlobs not implemented")
}

func (s *storage) StatBlobs(dest chan<- blob.SizedRef, blobs []blob.Ref) (err error) {
	for _, br := range blobs {
		m, err2 := s.meta(br)
		if err2 != nil {
			if err2 != os.ErrNotExist {
				err = err2
			}
		}
		dest <- m.SizedRef(br)
	}
	return nil
}

func (s *storage) EnumerateBlobs(dest chan<- blob.SizedRef, after string, limit int) (err error) {
	t := s.index.Find(after)
	for i := 0; i < limit && t.Next(); i++ {
		br, ok := blob.Parse(t.Key())
		if !ok {
			err = fmt.Errorf("diskpacked: couldn't parse index key %q", t.Key())
			continue
		}
		m, ok := parseBlobMeta(t.Value())
		if !ok {
			err = fmt.Errorf("diskpacked: couldn't parse index value %q: %q", t.Key(), t.Value())
			continue
		}
		dest <- m.SizedRef(br)
	}
	close(dest)
	return
}

func (s *storage) ReceiveBlob(br blob.Ref, source io.Reader) (brGot blob.SizedRef, err error) {
	tempFile, err := ioutil.TempFile("", br.String())
	if err != nil {
		return
	}
	defer func() {
		if e2 := tempFile.Close(); err == nil && e2 != nil {
			err = e2
		}
		os.Remove(tempFile.Name())
	}()

	hash := br.Hash()
	written, err := io.Copy(io.MultiWriter(hash, tempFile), source)
	if err != nil {
		return
	}
	if err = tempFile.Sync(); err != nil {
		return
	}
	stat, err := tempFile.Stat()
	if err != nil {
		return
	}
	if stat.Size() != written {
		err = fmt.Errorf("temp file %q size %d didn't match written size %d", tempFile.Name(), stat.Size(), written)
		return
	}
	if !br.HashMatches(hash) {
		err = blobserver.ErrCorruptBlob
		return
	}

	if _, err = tempFile.Seek(0, 0); err != nil {
		return
	}
	sbr := blob.SizedRef{Ref: br, Size: stat.Size()}
	if err = s.append(sbr, tempFile); err != nil {
		return
	}
	return sbr, nil
}

// append writes the provided blob to the current data file.
func (s *storage) append(br blob.SizedRef, r io.Reader) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("diskpacked: write to closed storage")
	}
	if err := s.openCurrent(); err != nil {
		return err
	}
	fmt.Fprintf(s.current, "[%v %v]", br.Ref.String(), br.Size)
	offset, err := s.current.Seek(0, 1)
	if err != nil {
		return err
	}
	n, err := io.Copy(s.current, r)
	if err != nil {
		return err
	}
	if n != br.Size {
		return fmt.Errorf("written blob size %d didn't match size %d", n, br.Size)
	}
	if err = s.current.Sync(); err != nil {
		return err
	}
	return s.index.Set(br.Ref.String(), blobMeta{s.currentN, offset, br.Size}.String())
}

// meta fetches the metadata for the specified blob from the index.
func (s *storage) meta(br blob.Ref) (m blobMeta, err error) {
	ms, err := s.index.Get(br.String())
	if err != nil {
		if err == index.ErrNotFound {
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
	file, offset, size int64
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
