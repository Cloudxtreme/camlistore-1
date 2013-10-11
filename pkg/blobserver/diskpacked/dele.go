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
	"errors"
	"fmt"
	"io"
	"os"

	"camlistore.org/pkg/blob"
)

var errNoPunch = errors.New("punchHole not supported")

// punchHole, if non-nil, punches a hole in f from offset to offset+size.
var punchHole func(file *os.File, offset, size int64) error

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

    if punchHole != nil {
	err = punchHole(f, meta.offset, meta.size)
	if err == nil {
		return nil
	}
    if err == errNoPunch {
        punchHole = nil
    } else {
		return err
	}}

	// fill with zero
	n, err := f.Seek(meta.offset, os.SEEK_SET)
	if err != nil {
		return err
	}
	if n != meta.offset {
		return fmt.Errorf("error seeking to %d: got %d", meta.offset, n)
	}
	_, err = io.CopyN(f, newZeroReader(meta.size), meta.size)
	if err != nil {
		return err
	}
	return nil
}

type zeroReader struct{}

func newZeroReader(n int64) io.Reader {
	return &io.LimitedReader{R: zeroReader{}, N: n}
}

func (z zeroReader) Read(p []byte) (n int, err error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}
