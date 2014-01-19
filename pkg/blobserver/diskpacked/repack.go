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

package diskpacked

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/sorted"
	"camlistore.org/pkg/sorted/kvfile"
	"camlistore.org/third_party/github.com/camlistore/lock"
)

const separator = "\nDISKPCK"

var ErrUnknownVersion = errors.New("unknown version")
var verbose = false

func init() {
	verbose = os.Getenv("CAMLI_DEBUG") != ""
}

// Repack rewrites the diskpacked .pack files
func Repack(root string) (err error) {
	// there is newStorage, but that may open a file for writing
	var s = &storage{root: root}
	index, err := kvfile.NewStorage(filepath.Join(root, "index.kv"))
	if err != nil {
		return err
	}
	defer func() {
		closeErr := index.Close()
		// just returning the first error - if the index or disk is corrupt
		// and can't close, it's very likely these two errors are related and
		// have the same root cause.
		if err == nil {
			err = closeErr
		}
	}()

	for i := 0; i >= 0; i++ {
		fh, err := os.Open(s.filename(i))
		if err != nil {
			if os.IsNotExist(err) {
				break
			}
			return err
		}
		err = repackOne(index, fh, fh.Name(), i)
		fh.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// repackOne reads from r pack source (any version), and writes a ".new" version
// of it, renaming the old file with .old extension.
func repackOne(index sorted.KeyValue, r io.ReadSeeker, name string, packId int) error {
	l, err := lock.Lock(name + ".lock")
	defer l.Close()

	batch := index.BeginBatch()

	out, lck, _, err := newFile(name + ".new")
	if err != nil {
		return fmt.Errorf("cannot create %q: %v", name+".new", err)
	}
	defer out.Close()
	defer lck.Close()

	batchCount := 0
	if err = walkOne(r, func(header header, data io.Reader) error {
		offset, _, err := appendBlob(out, header.Ref, data)
		if err != nil {
			return err
		}
		if verbose {
			log.Printf("written %s at %d", header, offset)
		}
		batch.Set(header.Ref.String(), blobMeta{packId, offset, header.Ref.Size}.String())
		batchCount++
		if batchCount >= 1000 {
			if err = index.CommitBatch(batch); err != nil {
				return err
			}
			batch = index.BeginBatch()
			batchCount = 0
		}
		return nil
	}); err != nil {
		out.Close()
		os.Remove(out.Name())
		return fmt.Errorf("diskpacked: error walking %q: %v", name, err)
	}
	if err = index.CommitBatch(batch); err != nil {
		return fmt.Errorf("diskpacked: error commiting finishing batch mutation: %v", err)
	}
	if err = out.Close(); err != nil {
		return fmt.Errorf("diskpacked: error closing newly written pack file %q: %v", out.Name(), err)
	}
	if err = os.Rename(name, name+".old"); err != nil {
		return fmt.Errorf("diskpacked: error renaming %q to %q: %v", name, name+".old", err)
	}
	return os.Rename(name+".new", name)
}

// walkOne walks the .pack file given as an io.ReadSeeker and handles each
// item to the walker function as a header and an io.Reader.
// Does this by first checking the version and then calling the appropriate walkOneVX function.
func walkOne(r io.ReadSeeker, walker func(head header, data io.Reader) error) error {
	ver, err := getVersion(r)
	if err != nil {
		return err
	}
	switch ver {
	case 0:
		return walkOneV0(r, walker)
	case 1:
		return walkOneV1(r, walker)
	default:
		return ErrUnknownVersion
	}
}

// getVersion returns the version number of the io.ReadSeeker.
// Seeks back to the original position unless a proper version tag found.
// If a proper version tag found, then seeks right after the version tag.
// If the file is empty/too short to have a version tag, io.EOF is returned.
func getVersion(r io.ReadSeeker) (int, error) {
	p, err := r.Seek(0, os.SEEK_CUR)
	if err != nil {
		return -1, fmt.Errorf("error getting position of %v: %v", r, err)
	}
	b := make([]byte, 2)
	if _, err = io.ReadFull(r, b); err != nil {
		//log.Printf("error reading first 2 bytes of %s: %v", r, err)
		if err == io.EOF {
			return -1, err
		}
		return -1, fmt.Errorf("error reading first 2 bytes of %v: %v", r, err)
	}
	switch string(b) {
	case "[s":
		if _, err = r.Seek(p, os.SEEK_SET); err != nil {
			return -1, fmt.Errorf("error seeking back to %d on %v: %v", p, r, err)
		}
		return 0, nil
	case "V1":
		return 1, nil
	default:
		if _, err = r.Seek(p, os.SEEK_SET); err != nil {
			return -1, fmt.Errorf("error seeking back to %d on %v: %v", p, r, err)
		}
		return -1, ErrUnknownVersion
	}
}

type header struct {
	Ref                      blob.SizedRef
	Compression              uint8
	Size                     uint32
	HeaderOffset, DataOffset int64
}

// walkOneV0 walks the .pack Version 0 file given as an io.ReadSeeker
// and handles each item to the walker function as a header and an io.Reader.
func walkOneV0(r io.ReadSeeker, walker func(head header, data io.Reader) error) error {
	var pos, headerOffset int64
	var originalSize uint32
	var compression uint8

	errAt := func(prefix, suffix string) error {
		if prefix != "" {
			prefix = prefix + " "
		}
		if suffix != "" {
			suffix = " " + suffix
		}
		return fmt.Errorf(prefix+"at %d (0x%x) in %q:"+suffix, pos, pos, r)
	}

	br := bufio.NewReader(r)
	for {
		headerOffset = pos
		if b, err := br.ReadByte(); err != nil {
			if err == io.EOF {
				break
			}
			return errAt("error while reading", err.Error())
		} else if b != '[' {
			return errAt(fmt.Sprintf("found byte 0x%x", b), "but '[' should be here!")
		}
		chunk, err := br.ReadSlice(']')
		if err != nil {
			if err == io.EOF {
				break
			}
			return errAt("error reading blob header", err.Error())
		}
		m := len(chunk)
		chunk = chunk[:m-1]
		i := bytes.IndexByte(chunk, byte(' '))
		if i <= 0 {
			return errAt("", fmt.Sprintf("bad header format (no space in %q)", chunk))
		}
		j := bytes.IndexByte(chunk[i+1:], byte(' '))
		var size uint64
		if j > 0 { // compression method + original size
			j += i + 2
			k := bytes.IndexByte(chunk[j+1:], byte(' '))
			if k < 0 {
				return errAt("", fmt.Sprintf("bad header format (space after size, but not two numbers)"))
			}
			if size, err = strconv.ParseUint(string(chunk[j:k]), 10, 8); err != nil {
				return errAt(fmt.Sprintf("cannot parse compression %q as int", chunk[j:k]), err.Error())
			}
			compression = uint8(size)
			if size, err = strconv.ParseUint(string(chunk[k+1:]), 10, 64); err != nil {
				return errAt(fmt.Sprintf("cannot parse original size %q as int", chunk[k:]), err.Error())
			}
			originalSize = uint32(size)
		} else {
			j = len(chunk)
			compression = 0
		}
		if size, err = strconv.ParseUint(string(chunk[i+1:j]), 10, 32); err != nil {
			return errAt(fmt.Sprintf("cannot parse size %q as int", chunk[i+1:]), err.Error())
		}
		ref, ok := blob.Parse(string(chunk[:i]))
		if !ok {
			return errAt("", fmt.Sprintf("cannot parse %q as blobref", chunk[:i]))
		}
		if verbose {
			log.Printf("found %s at %d", ref, pos)
		}
		pos += 1 + int64(m)

		if ref.Valid() { // not deleted
			if compression == 0 {
				originalSize = uint32(size)
			}
			hdr := header{Ref: blob.SizedRef{ref, originalSize},
				Size: uint32(size), Compression: compression,
				HeaderOffset: headerOffset, DataOffset: pos}

			if compression != 0 {
				return errors.New("compression is not implemented")
			}

			if _, err = r.Seek(pos, os.SEEK_SET); err != nil {
				return errAt("", "cannot seek to "+strconv.FormatInt(pos, 10))
			}
			if err = walker(hdr, &io.LimitedReader{r, int64(size)}); err != nil {
				return err
			}
		}

		if pos, err = r.Seek(pos+int64(size), os.SEEK_SET); err != nil {
			return errAt("", "cannot seek +"+strconv.FormatUint(size, 10)+" bytes")
		}
		// drain the buffer after the underlying reader Seeks
		io.CopyN(ioutil.Discard, br, int64(br.Buffered()))
	}
	return nil
}

// walkOneV1 walks the .pack Version 1 file given as an io.ReadSeeker
// and handles each item to the walker function as a header and an io.Reader.
func walkOneV1(r io.ReadSeeker, walker func(head header, data io.Reader) error) error {
	pos, err := r.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}
	var size, headerOffset int64

	errAt := func(prefix, suffix string) error {
		if prefix != "" {
			prefix = prefix + " "
		}
		if suffix != "" {
			suffix = " " + suffix
		}
		return fmt.Errorf(prefix+"at %d (0x%x) in %q:"+suffix, pos, pos, r)
	}

	var it item
	sep := make([]byte, 0, 128)
	br := bufio.NewReader(r)
	for {
		sep = sep[:9]
		if _, err = io.ReadFull(br, sep); err != nil {
			if err == io.EOF {
				break
			}
			return errAt("error reading separator", err.Error())
		}
		if !bytes.Equal(sep[:8], []byte(separator)) {
			return errAt(fmt.Sprintf("not separator %s found, but %q", separator, sep), "")
		}
		pos += int64(len(sep))
		headerOffset = pos
		headerLength := int(sep[8])
		if cap(sep) < headerLength {
			sep = make([]byte, headerLength)
		} else {
			sep = sep[:headerLength]
		}
		if _, err = io.ReadFull(br, sep); err != nil {
			return errAt("error reading header",
				fmt.Sprint("wanted to read %d bytes, got %v", headerLength, err))
		}
		it.Compression, it.UncomprSize = 0, 0
		if err = json.Unmarshal(sep, &it); err != nil {
			return errAt("error reading header", err.Error())
		}
		if verbose {
			log.Printf("found %#v at %d", it, pos)
		}
		pos += int64(headerLength)

		var ref blob.Ref
		if err = (&ref).UnmarshalBinary(it.Ref); err != nil {
			return errAt("", fmt.Sprintf("cannot unmarshal %x to ref: %v", it.Ref, err))
		}
		size = int64(it.Size)
		if ref.Valid() { // not deleted
			if it.Compression == 0 {
				it.UncomprSize = it.Size
			}
			hdr := header{Ref: blob.SizedRef{ref, it.UncomprSize},
				Compression: it.Compression, Size: it.Size,
				HeaderOffset: headerOffset, DataOffset: pos}

			if it.Compression != 0 {
				return errors.New("compression is not implemented")
			}

			if _, err = r.Seek(pos, os.SEEK_SET); err != nil {
				return errAt("", "cannot seek to "+strconv.FormatInt(pos, 10))
			}
			if err = walker(hdr, &io.LimitedReader{r, size}); err != nil {
				return err
			}
		}

		if pos, err = r.Seek(pos+size, os.SEEK_SET); err != nil {
			return errAt("", fmt.Sprintf("cannot seek +%d bytes", size))
		}
		// drain the buffer after the underlying reader Seeks
		io.CopyN(ioutil.Discard, br, int64(br.Buffered()))
	}
	return nil
}

func newFile(filename string) (fh *os.File, lck io.Closer, off int64, err error) {
	if lck, err = lock.Lock(filename + ".lock"); err != nil {
		return
	}
	if fh, err = os.Create(filename); err != nil {
		lck.Close()
		return
	}
	// write version tag
	n, err := writeVersionTag(fh)
	if err != nil {
		return
	}
	return fh, lck, int64(n), nil
}

// writeVersionTag writes the current version's tag to the given io.Writer
func writeVersionTag(w io.Writer) (n int, err error) {
	if CurrentVersion > 9 {
		return 0, errors.New("version number too big")
	}
	return w.Write([]byte{'V', byte('0') + CurrentVersion})
}
