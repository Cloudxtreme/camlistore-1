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
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"

	"camlistore.org/pkg/blob"
)

var errNoPunch = errors.New("punchHole not supported")

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

	switch s.version {
	case 0:
		return deleteV0(f, br, meta)
	case 1:
		return deleteV1(f, br, meta)
	default:
		return ErrUnknownVersion
	}
}

func deleteV0(f *os.File, br blob.Ref, meta blobMeta) error {
	// walk back, find the header, and overwrite the hash with xxxx-000000...
	k := 1 + len(br.String()) + 1 + len(strconv.FormatUint(uint64(meta.size), 10)) + 1
	off := meta.offset - int64(k)
	b := make([]byte, k)
	k, err := f.ReadAt(b, off)
	if err != nil {
		return err
	}
	if b[0] != byte('[') || b[k-1] != byte(']') {
		return fmt.Errorf("delete: cannot find header surroundings, found %q", b)
	}
	// TODO(tgulacsi): check whether this sha1-xxx is the same as br!
	b = b[1 : k-1] // "sha1-xxxxxxxxxxxxxxxxxx nnnn" - everything between []
	off += 1

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

	return writeBackAndZero(f, b, off, meta)
}

func deleteV1(f *os.File, br blob.Ref, meta blobMeta) error {
	// walk back, find the header, and overwrite the hash with xxxx-000000...
	k := 256
	off := meta.offset - int64(k)
	if off < 0 {
		k += int(off)
		off = 0
	}
	off, err := f.Seek(off, os.SEEK_SET)
	if err != nil {
		return err
	}
	b := make([]byte, k)
	if k, err = io.ReadFull(f, b); err != nil {
		return err
	}
	if k < len(b) {
		b = b[:k]
	}
	if b[len(b)-1] != '}' {
		return fmt.Errorf("delete: cannot find header end, found %q", b)
	}
	if k = bytes.LastIndex(b, []byte(separator)); k < 0 {
		return fmt.Errorf("delete: cannot find header begin, found %q", b)
	}
	b = b[k+len(separator)+1:]
	if b[0] != '{' {
		return fmt.Errorf("delete: bad header begin, found %q", b)
	}
	if k = bytes.Index(b, []byte("\"r\":\"")); k < 0 {
		return fmt.Errorf("delete: cannot find \"r\":\" in header %q", b)
	}
	b = b[k+5:]
	off = meta.offset - int64(len(b))
	if k = bytes.IndexByte(b, '"'); k < 0 {
		return fmt.Errorf("delete: cannot find ending \" in header %q", b)
	}
	// base64-encoded sha1-xxxxxxxxxxxxxxxxxxxx
	brb := make([]byte, base64.StdEncoding.DecodedLen(k))
	n, err := base64.StdEncoding.Decode(brb, b[:k])
	if err != nil {
		return fmt.Errorf("delete: bad ref (%q): %v", b, err)
	}
	if n != len(brb) {
		brb = brb[:n]
	}
	br2 := &blob.Ref{}
	if err = br2.UnmarshalBinary(brb); err != nil {
		return fmt.Errorf("delete: cannot unmarshal %q as blob ref: %v", brb, err)
	}
	if *br2 != br {
		return fmt.Errorf("delete: found %s in place of %s", br2, br)
	}

	// Replace b with "xxxx-000000000"
	dash := bytes.IndexByte(brb, '-')
	if dash < 0 {
		return fmt.Errorf("delete: cannot find dash in ref %q", b)
	}
	for i := 0; i < dash; i++ {
		brb[i] = 'x'
	}
	for i := dash + 1; i < len(brb); i++ {
		brb[i] = 0
	}
	base64.StdEncoding.Encode(b, brb)

	return writeBackAndZero(f, b, off, meta)
}

func writeBackAndZero(f *os.File, b []byte, off int64, meta blobMeta) error {
	// write back
	_, err := f.WriteAt(b, off)
	if err != nil {
		return err
	}

	// punch hole, if possible
	if punchHole != nil {
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
	_, err = io.CopyN(f, zeroReader{}, int64(meta.size))
	return err
}

type zeroReader struct{}

func (z zeroReader) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}
