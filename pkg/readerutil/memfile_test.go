/*
Copyright 2013 the Camlistore authors.

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

package readerutil

import (
	"io"
	"io/ioutil"
	"testing"

	"camlistore.org/pkg/types"
)

// Make an io.ReadSeeker + io.Closer by reading the whole reader.
func MakeReadSeekCloser(r io.Reader) (types.ReadSeekCloser, error) {
	ms := NewMemorySlurper()
	// you should call ms.Close() to clean up temp file
	if _, err := io.Copy(ms, r); err != nil {
		return nil, err
	}
	return ms, nil
}

func TestMakeReadSeekCloser(t *testing.T) {
	maxSize := maxInMemorySlurp
	for i := 0; i < 2; i++ {
		m := int64(maxSize + i)
		rsc, err := MakeReadSeekCloser(&io.LimitedReader{zeroReader{}, m})
		if err != nil {
			t.Errorf("error with less than maxSize: %v", err)
		}
		defer rsc.Close()

		ms := rsc.(*memorySlurper)
		if i == 0 {
			if ms.file != nil {
				t.Errorf("tempfile with small size! %#v", ms)
			}
		} else if ms.file == nil {
			t.Errorf("no tempfile with big size! %#v", ms)
		}

		n, err := rsc.Seek(0, 2)
		if err != nil {
			t.Errorf("error seeking %v: %v", rsc, err)
		}
		if n != m {
			t.Errorf("seek end is %d, but wanted %d", n, m)
		}
		if _, err = rsc.Seek(0, 0); err != nil {
			t.Errorf("cannot seek to beginning: %v", err)
		}
		if n, err = io.Copy(ioutil.Discard, rsc); err != nil {
			t.Errorf("error reading from rsc: %v", err)
		}
		if n != m {
			t.Errorf("wanted to read %d, but got %d", m, n)
		}
	}
}

type zeroReader struct{}

func (z zeroReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}
