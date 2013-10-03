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
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/index"
	"camlistore.org/pkg/index/kvfile"
	"camlistore.org/third_party/github.com/camlistore/lock"
)

func Reindex(root string, overwrite, verbose bool) error {
	var (
		err     error
		fh      *os.File
		s       = &storage{root: root}
	)
	indx, closer, err := kvfile.NewStorage(filepath.Join(root, "index.kv"))
	if err != nil {
		return err
	}
	defer closer.Close()

	for i := int64(0); i >= 0; i++ {
		if fh, err = os.Open(s.filename(i)); err != nil {
			if os.IsNotExist(err) {
				break
			}
			return err
		}
		err = reindexOne(indx, overwrite, verbose, fh, fh.Name(), i)
		fh.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func reindexOne(indx index.Storage, overwrite, verbose bool, r io.ReadSeeker, name string, packId int64) error {
	l, err := lock.Lock(name + ".lock")
	defer l.Close()

	var (
		b            byte
		meta, old    string
		chunk        []byte
		pos, size    int64
		i, m         int
		ref          blob.Ref
		ok           bool
		br           *bufio.Reader
		batch        index.BatchMutation
		everythingok = true
	)

	errAt := func(prefix, suffix string) error {
		if prefix != "" {
			prefix = prefix + " "
		}
		if suffix != "" {
			suffix = " " + suffix
		}
		return fmt.Errorf(prefix+"at %d (0x%x) in %q:"+suffix, pos, pos, name)
	}
	if overwrite {
		batch = indx.BeginBatch()
	}

	for {
		br = bufio.NewReader(r)
		if b, err = br.ReadByte(); err != nil {
			if err == io.EOF {
				break
			}
			return errAt("error while reading", err.Error())
		} else if b != '[' {
			return errAt(fmt.Sprintf("found byte 0x%x", b), "but '[' should be here!")
		}
		if chunk, err = br.ReadSlice(']'); err != nil {
			if err == io.EOF {
				break
			}
			return errAt("error reading blob header", err.Error())
		}
		m, chunk = len(chunk), chunk[:len(chunk)-1]
		if i = bytes.IndexByte(chunk, byte(' ')); i <= 0 {
			return errAt("", fmt.Sprintf("bad header format (no space in %q)", chunk))
		}
		if size, err = strconv.ParseInt(string(chunk[i+1:]), 10, 64); err != nil {
			return errAt(fmt.Sprintf("cannot parse size %q as int", chunk[i+1:]), err.Error())
		}
		if ref, ok = blob.Parse(string(chunk[:i])); !ok {
			return errAt("", fmt.Sprintf("cannot parse %q as blobref", chunk[:i]))
		}
		if verbose {
			log.Printf("found %s at %d", ref, pos)
		}

		meta = blobMeta{packId, pos + 1 + int64(m), size}.String()
		if overwrite && batch != nil {
			batch.Set(ref.String(), meta)
		} else {
			if old, err = indx.Get(ref.String()); err != nil {
				everythingok = false
				if err == index.ErrNotFound {
					log.Println(ref.String() + ": cannot find in index!")
				} else {
					log.Println(ref.String()+": error getting from index: ", err.Error())
				}
			} else if old != meta {
				everythingok = false
				log.Println(ref.String() + ": index mismatch - index=" + old + " data=" + meta)
			}
		}

		pos += 1 + int64(m)
		// TODO(tgulacsi78): not just seek, but check the hashes of the files
		if pos, err = r.Seek(pos+size, 0); err != nil {
			return errAt("", "cannot seek +"+strconv.FormatInt(size, 10)+" bytes")
		}
	}

	if overwrite && batch != nil {
		log.Printf("overwriting %s from %s", indx, name)
		if err = indx.CommitBatch(batch); err != nil {
			return err
		}
	} else if !everythingok {
		return fmt.Errorf("index does not match data in %q", name)
	}
	return nil
}
