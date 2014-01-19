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
	"bytes"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/blobserver"
	"camlistore.org/pkg/blobserver/local"
	"camlistore.org/pkg/context"
	"camlistore.org/pkg/jsonconfig"
	"camlistore.org/pkg/sorted"
	"camlistore.org/pkg/sorted/kvfile"
	"camlistore.org/pkg/syncutil"
	"camlistore.org/pkg/types"
	"camlistore.org/third_party/github.com/camlistore/lock"
)

// TODO(wathiede): replace with glog.V(2) when we decide our logging story.
type debugT bool

var debug = debugT(false)

func (d debugT) Printf(format string, args ...interface{}) {
	if bool(d) {
		log.Printf(format, args...)
	}
}

func (d debugT) Println(args ...interface{}) {
	if bool(d) {
		log.Println(args...)
	}
}

// CurrentVersion is the version of the diskpacked file format
const CurrentVersion = 1

const defaultMaxFileSize = 512 << 20 // 512MB

type storage struct {
	root        string
	index       sorted.KeyValue
	maxFileSize int64
	version     int

	writeLock io.Closer // Provided by lock.Lock, and guards other processes from accesing the file open for writes.

	mu     sync.Mutex // Guards all I/O state.
	closed bool
	writer *os.File
	fds    []*os.File
	size   int64

	*local.Generationer
}

func (s *storage) String() string {
	return fmt.Sprintf("\"diskpacked\" blob packs at %s", s.root)
}

var (
	readVar     = expvar.NewMap("diskpacked-read-bytes")
	readTotVar  = expvar.NewMap("diskpacked-total-read-bytes")
	openFdsVar  = expvar.NewMap("diskpacked-open-fds")
	writeVar    = expvar.NewMap("diskpacked-write-bytes")
	writeTotVar = expvar.NewMap("diskpacked-total-write-bytes")
)

const indexKV = "index.kv"

// IsDir reports whether dir is a diskpacked directory.
func IsDir(dir string) (bool, error) {
	_, err := os.Stat(filepath.Join(dir, indexKV))
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
}

// New returns a diskpacked storage implementation, adding blobs to
// the provided directory. It doesn't delete any existing blob pack
// files.
func New(dir string) (blobserver.Storage, error) {
	var maxSize int64
	if ok, _ := IsDir(dir); ok {
		// TODO: detect existing max size from size of files, if obvious,
		// and set maxSize to that?
	}
	return newStorage(dir, maxSize, nil)
}

var errBadVersion = errors.New("bad pack version")

type item struct {
	Ref         []byte `json:"r"`           // as Ref.MarshalBinary
	Size        uint32 `json:"s"`           // on-disk (compressed) size
	Compression uint8  `json:"c,omitempty"` // compression method
	UncomprSize uint32 `json:"u,omitempty"` // original (uncompressed) size
}

// Encode item (diskpacked item header) as
// "DISKPACKED" + uint8(headerLength) + json-encoded header
func (it item) Encode(w io.Writer) (n int, err error) {
	if n, err = io.WriteString(w, separator); err != nil {
		return
	}
	buf := bytes.NewBuffer(make([]byte, 0, 128))
	if err = json.NewEncoder(buf).Encode(it); err != nil {
		return
	}
	b := buf.Bytes()
	if b[len(b)-1] == '\n' {
		b = b[:len(b)-1]
	}
	if len(b) > 256 { // the limit with uint8
		return 0, fmt.Errorf("Encode: header item too long (%d)", len(b))
	}
	if bw, ok := w.(io.ByteWriter); ok {
		err = bw.WriteByte(uint8(len(b)))
	} else {
		_, err = w.Write([]byte{uint8(len(b))})
	}
	if err != nil {
		return
	}
	n++

	m, err := w.Write(b)
	n += m
	return
}

// newStorage returns a new storage in path root with the given maxFileSize,
// or defaultMaxFileSize (512MB) if <= 0
func newStorage(root string, maxFileSize int64, indexConf jsonconfig.Obj) (s *storage, err error) {
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
	var index sorted.KeyValue
	if len(indexConf) > 0 {
		index, err = sorted.NewKeyValue(indexConf)
	} else {
		index, err = kvfile.NewStorage(filepath.Join(root, indexKV))
	}
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			index.Close()
		}
	}()
	if maxFileSize <= 0 {
		maxFileSize = defaultMaxFileSize
	}
	// Be consistent with trailing slashes.  Makes expvar stats for total
	// reads/writes consistent across diskpacked targets, regardless of what
	// people put in their low level config.
	root = strings.TrimRight(root, `\/`)
	s = &storage{
		root:         root,
		index:        index,
		maxFileSize:  maxFileSize,
		Generationer: local.NewGenerationer(root),
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.openAllPacks(); err != nil {
		return nil, err
	}
	if _, _, err := s.StorageGeneration(); err != nil {
		return nil, fmt.Errorf("Error initialization generation for %q: %v", root, err)
	}
	return s, nil
}

func newFromConfig(_ blobserver.Loader, config jsonconfig.Obj) (storage blobserver.Storage, err error) {
	var (
		path        = config.RequiredString("path")
		maxFileSize = config.OptionalInt("maxFileSize", 0)
		indexConf   = config.OptionalObject("metaIndex")
	)
	if err := config.Validate(); err != nil {
		return nil, err
	}
	return newStorage(path, int64(maxFileSize), indexConf)
}

func init() {
	blobserver.RegisterStorageConstructor("diskpacked", blobserver.StorageConstructor(newFromConfig))
}

// openForRead will open pack file n for read and keep a handle to it in
// s.fds.  os.IsNotExist returned if n >= the number of pack files in s.root.
// This function is not thread safe, s.mu should be locked by the caller.
func (s *storage) openForRead(n int) error {
	if n > len(s.fds) {
		panic(fmt.Sprintf("openForRead called out of order got %d, expected %d", n, len(s.fds)))
	}

	fn := s.filename(n)
	f, err := os.Open(fn)
	if err != nil {
		return err
	}
	s.version, err = getVersion(f)
	if err != nil {
		return err
	}
	openFdsVar.Add(s.root, 1)
	debug.Printf("diskpacked: opened for read %q", fn)
	s.fds = append(s.fds, f)
	return nil
}

// openForWrite will create or open pack file n for writes, create a lock
// visible external to the process and seek to the end of the file ready for
// appending new data.
// This will return errBadVersion if the pack file exists and has different
// version as we will write.
// This function is not thread safe, s.mu should be locked by the caller.
func (s *storage) openForWrite(n int) error {
	fn := s.filename(n)
	l, err := lock.Lock(fn + ".lock")
	if err != nil {
		return err
	}
	f, err := os.OpenFile(fn, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		l.Close()
		return err
	}
	if v, err := getVersion(f); err != nil {
		if err == io.EOF { // empty file
			if err = f.Truncate(0); err != nil {
				l.Close()
				return err
			}
			// truncate and go on
		} else {
			f.Close()
			l.Close()
			return err
		}
	} else if v < CurrentVersion {
		log.Printf("file %q has version %d, but current version is %d", v, CurrentVersion)
		f.Close()
		l.Close()
		return errBadVersion
	} else {
		s.version = v
	}
	openFdsVar.Add(s.root, 1)
	debug.Printf("diskpacked: opened for write %q", fn)

	s.size, err = f.Seek(0, os.SEEK_END)
	if err != nil {
		return err
	}
	if s.size == 0 {
		n, err := writeVersionTag(f)
		if err != nil {
			return err
		}
		s.size += int64(n)
	}

	s.writer = f
	s.writeLock = l
	return nil
}

// nextPack will close the current writer and release its lock if open,
// open the next pack file in sequence for writing, grab its lock, set it
// to the currently active writer, and open another copy for read-only use.
// This function is not thread safe, s.mu should be locked by the caller.
func (s *storage) nextPack() error {
	debug.Println("diskpacked: nextPack")
	s.size = 0
	if s.writeLock != nil {
		err := s.writeLock.Close()
		if err != nil {
			return err
		}
		s.writeLock = nil
	}
	if s.writer != nil {
		if err := s.writer.Close(); err != nil {
			return err
		}
		openFdsVar.Add(s.root, -1)
	}

	n := len(s.fds)
	if err := s.openForWrite(n); err != nil {
		if err == errBadVersion { // try again
			// open "old" for reading, and create a new for writing
			if err = s.openForRead(n); err == nil {
				n++
				err = s.openForWrite(n)
			}
		}
		if err != nil {
			return err
		}
	}
	return s.openForRead(n)
}

// openAllPacks opens read-only each pack file in s.root, populating s.fds.
// The latest pack file will also have a writable handle opened.
// This function is not thread safe, s.mu should be locked by the caller.
func (s *storage) openAllPacks() error {
	debug.Println("diskpacked: openAllPacks")
	n := 0
	for {
		err := s.openForRead(n)
		if os.IsNotExist(err) {
			break
		}
		if err != nil {
			s.Close()
			return err
		}
		n++
	}

	if n == 0 {
		// If no pack files are found, we create one open for read and write.
		return s.nextPack()
	}

	// If 1 or more pack files are found, open the last one read and write.
	err := s.openForWrite(n - 1)
	if err == errBadVersion { // try again
		// open the "old" for reading, and create a new for writing
		if err = s.openForRead(n - 1); err != nil {
			s.Close()
			return err
		}
		err = s.openForWrite(n)
	}
	return err
}

func (s *storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var closeErr error
	if !s.closed {
		s.closed = true
		if err := s.index.Close(); err != nil {
			log.Println("diskpacked: closing index:", err)
		}
		for _, f := range s.fds {
			if err := f.Close(); err != nil {
				closeErr = err
			}
			openFdsVar.Add(s.root, -1)
		}
		s.writer = nil
		if l := s.writeLock; l != nil {
			err := l.Close()
			if closeErr == nil {
				closeErr = err
			}
			s.writeLock = nil
		}
	}
	return closeErr
}

func (s *storage) Fetch(br blob.Ref) (io.ReadCloser, uint32, error) {
	meta, err := s.meta(br)
	if err != nil {
		return nil, 0, err
	}

	if meta.file >= len(s.fds) {
		return nil, 0, fmt.Errorf("diskpacked: attempt to fetch blob from out of range pack file %d > %d", meta.file, len(s.fds))
	}
	rac := s.fds[meta.file]
	var rs io.ReadSeeker = io.NewSectionReader(rac, meta.offset, int64(meta.size))
	fn := rac.Name()
	// Ensure entry is in map.
	readVar.Add(fn, 0)
	if v, ok := readVar.Get(fn).(*expvar.Int); ok {
		rs = types.NewStatsReadSeeker(v, rs)
	}
	readTotVar.Add(s.root, 0)
	if v, ok := readTotVar.Get(s.root).(*expvar.Int); ok {
		rs = types.NewStatsReadSeeker(v, rs)
	}
	rsc := struct {
		io.ReadSeeker
		io.Closer
	}{
		rs,
		types.NopCloser,
	}
	return rsc, meta.size, nil
}

func (s *storage) filename(file int) string {
	return filepath.Join(s.root, fmt.Sprintf("pack-%05d.blobs", file))
}

var removeGate = syncutil.NewGate(20) // arbitrary

// RemoveBlobs removes the blobs from index and pads data with zero bytes
func (s *storage) RemoveBlobs(blobs []blob.Ref) error {
	batch := s.index.BeginBatch()
	var wg syncutil.Group
	for _, br := range blobs {
		br := br
		removeGate.Start()
		batch.Delete(br.String())
		wg.Go(func() error {
			defer removeGate.Done()
			if err := s.delete(br); err != nil {
				return err
			}
			return nil
		})
	}
	err1 := wg.Err()
	err2 := s.index.CommitBatch(batch)
	if err1 != nil {
		return err1
	}
	return err2
}

var statGate = syncutil.NewGate(20) // arbitrary

func (s *storage) StatBlobs(dest chan<- blob.SizedRef, blobs []blob.Ref) (err error) {
	var wg syncutil.Group

	for _, br := range blobs {
		br := br
		statGate.Start()
		wg.Go(func() error {
			defer statGate.Done()

			m, err := s.meta(br)
			if err == nil {
				dest <- m.SizedRef(br)
				return nil
			}
			if err == os.ErrNotExist {
				return nil
			}
			return err
		})
	}
	return wg.Err()
}

func (s *storage) EnumerateBlobs(ctx *context.Context, dest chan<- blob.SizedRef, after string, limit int) (err error) {
	defer close(dest)

	t := s.index.Find(after, "")
	defer func() {
		closeErr := t.Close()
		if err == nil {
			err = closeErr
		}
	}()
	for i := 0; i < limit && t.Next(); {
		key := t.Key()
		if key <= after {
			// EnumerateBlobs' semantics are '>', but sorted.KeyValue.Find is '>='.
			continue
		}
		br, ok := blob.Parse(key)
		if !ok {
			return fmt.Errorf("diskpacked: couldn't parse index key %q", key)
		}
		m, ok := parseBlobMeta(t.Value())
		if !ok {
			return fmt.Errorf("diskpacked: couldn't parse index value %q: %q", key, t.Value())
		}
		select {
		case dest <- m.SizedRef(br):
		case <-ctx.Done():
			return context.ErrCanceled
		}
		i++
	}
	return nil
}

func (s *storage) ReceiveBlob(br blob.Ref, source io.Reader) (sbr blob.SizedRef, err error) {
	var b bytes.Buffer
	n, err := b.ReadFrom(source)
	if err != nil {
		return
	}

	sbr = blob.SizedRef{Ref: br, Size: uint32(n)}

	// Check if it's a dup. Still accept it if the pack file on disk seems to be corrupt
	// or truncated.
	if m, err := s.meta(br); err == nil {
		fi, err := os.Stat(s.filename(m.file))
		if err == nil && fi.Size() >= m.offset+int64(m.size) {
			return sbr, nil
		}
	}

	err = s.append(sbr, &b)
	return
}

// append writes the provided blob to the current data file.
func (s *storage) append(br blob.SizedRef, r io.Reader) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("diskpacked: write to closed storage")
	}

	offset, n, err := appendBlob(s.writer, br, r)
	if err != nil {
		return err
	}
	fn := s.writer.Name()
	s.size = offset + int64(br.Size)
	writeVar.Add(fn, n)
	writeTotVar.Add(s.root, n)

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

// appendBlob writes the provided blob to the provided data file
func appendBlob(w io.WriteSeeker, br blob.SizedRef, r io.Reader) (offset, written int64, err error) {
	p, e := w.Seek(0, os.SEEK_CUR)
	if e != nil {
		err = e
		return
	}
	refBytes, e := br.Ref.MarshalBinary()
	if e != nil {
		err = e
		return
	}
	n, err := item{Ref: refBytes, Size: uint32(br.Size)}.Encode(w)
	// TODO(adg): remove this seek and the offset check once confident
	if offset, err = w.Seek(0, os.SEEK_CUR); err != nil {
		return
	}
	if p+int64(n) != offset {
		err = fmt.Errorf("diskpacked: seek says offset = %d, we think %d", offset, p+int64(n))
		return
	}

	n2, err := io.Copy(w, r)
	if err != nil {
		return
	}
	if n2 != int64(br.Size) {
		err = fmt.Errorf("diskpacked: written blob size %d didn't match size %d", n2, br.Size)
		return
	}
	return offset, int64(n) + n2, nil
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
