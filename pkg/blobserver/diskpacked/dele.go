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
	"fmt"
	"io"
	"os"

	"camlistore.org/pkg/blob"
)

func (s *storage) delete(br blob.Ref) error {
	meta, err := s.meta(br)
	if err != nil {
		return err
	}
	f, err := openFile(s.filename(meta.file), true)
	if err != nil {
		return err
	}
	defer f.Close()

	// fill with zero
	n, err := f.Seek(meta.offset, os.SEEK_SET)
	if err != nil {
		return err
	}
	if n != meta.offset {
		return fmt.Errorf("error seeking to %d: got %d", meta.offset, n)
	}
	n, err = io.Copy(f, newZeroReader(meta.size))
	if err != nil {
		return err
	}
	if n != meta.size {
		return fmt.Errorf("error zeroing data: wanted %d, written %d", meta.size, n)
	}
	return nil
}

type zeroReader struct {
	n int64
}

func newZeroReader(n int64) *zeroReader {
	return &zeroReader{n}
}

func (z *zeroReader) Read(p []byte) (n int, err error) {
	m := z.n
	if m <= 0 {
		return 0, io.EOF
	}
	if m > int64(len(p)) {
		m = int64(len(p))
	}
	if m == 0 {
		return 0, io.EOF
	}
	n = int(m)
	for i := 0; i < n; i++ {
		p[i] = 0
	}
	z.n -= m
	return n, nil
}

// WriteTo writes data to w until there's no more data to write or when
// an error occurs. The return value n is the number of bytes written.
// Any error encountered during the write is also returned.
func (z *zeroReader) WriteTo(w io.Writer) (n int64, err error) {
	m := 1 << 20
	if int64(m) > z.n {
		m = int(z.n)
	}
	buf := make([]byte, m)
	for z.n > 0 {
		if int64(len(buf)) > z.n {
			buf = buf[:int(z.n)]
		}
		if m, err = w.Write(buf); err != nil {
			return
		}
		n += int64(m)
		z.n -= int64(m)
	}
	return
}
