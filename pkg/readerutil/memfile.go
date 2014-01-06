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
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"sync"

	"camlistore.org/pkg/types"
)

const maxInMemorySlurp = 4 << 20 // 4MB.  *shrug*

// memorySlurper slurps up a blob to memory (or spilling to disk if
// over maxInMemorySlurp) and deletes the file on Close
type memorySlurper struct {
	buf     *bytes.Buffer
	mem     *bytes.Reader
	file    *os.File // nil until allocated
	reading bool     // transitions at most once from false -> true
	sync.Mutex
}

// NewMemorySlurper returns a memory slurper, initialized for writing
// If more than maxInMemorySlurp bytes (4Mb) is written to the returned
// ReadWriteSeekCloser, then all data is written into a temporary file.
// This is called "spilling" here.
func NewMemorySlurper() types.ReadWriteSeekCloser {
	return &memorySlurper{buf: new(bytes.Buffer)}
}

// Read reads into p
// After calling Read, no Write can happen!
func (ms *memorySlurper) Read(p []byte) (n int, err error) {
	ms.Lock()
	defer ms.Unlock()

	if !ms.reading {
		ms.reading = true
		if ms.file != nil {
			ms.file.Seek(0, 0)
		} else if ms.buf != nil {
			ms.mem = bytes.NewReader(ms.buf.Bytes())
			ms.buf = nil
		} else {
			err = io.EOF
			return
		}
	}
	if ms.file != nil {
		return ms.file.Read(p)
	}
	return ms.mem.Read(p)
}

// Seek seeks to the given offset relative to whence
// After calling Seek, no Write can happen!
func (ms *memorySlurper) Seek(offset int64, whence int) (int64, error) {
	ms.Lock()
	defer ms.Unlock()

	if !ms.reading {
		ms.reading = true
		if ms.file == nil {
			ms.mem = bytes.NewReader(ms.buf.Bytes())
			ms.buf = nil
		} else {
			ms.file.Sync()
		}
	}
	if ms.file != nil {
		return ms.file.Seek(offset, whence)
	}
	return ms.mem.Seek(offset, whence)
}

// Write writes p to the slurper, returns the number of bytes written, and the error
// Writes to memory till the maxInMemorySlurp limit is reached,
// then spills the contents to a temporary file
func (ms *memorySlurper) Write(p []byte) (n int, err error) {
	ms.Lock()
	defer ms.Unlock()

	if ms.reading {
		panic("write after read")
	}
	if ms.file != nil {
		n, err = ms.file.Write(p)
		return
	}

	if ms.buf.Len()+len(p) > maxInMemorySlurp {
		ms.file, err = ioutil.TempFile("", "slurp-")
		if err != nil {
			return
		}
		removeEarly(ms.file.Name())

		_, err = io.Copy(ms.file, ms.buf)
		if err != nil {
			return
		}
		ms.buf = nil
		n, err = ms.file.Write(p)
		return
	}

	return ms.buf.Write(p)
}

// Close cleans up the tempfile if exists
func (ms *memorySlurper) Close() error {
	ms.Lock()
	defer ms.Unlock()

	fileName := ""
	if ms.file != nil {
		fileName = ms.file.Name()
		ms.file.Close()
	}
	ms.buf = nil
	ms.mem = nil
	ms.file = nil
	if fileName != "" {
		return removeAtlast(fileName)
	}
	return nil
}
