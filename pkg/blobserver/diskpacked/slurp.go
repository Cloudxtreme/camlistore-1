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
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	//"log"
	"os"

	"camlistore.org/pkg/blob"
)

const maxInMemorySlurp = 4 << 20 // 4MB.  *shrug*

// netSlurper slurps up a blob to memory (or spilling to disk if
// over maxInMemorySlurp) to verify its digest
type netSlurper struct {
	blob    blob.Ref // only used for tempfile's prefix
	buf     *bytes.Buffer
	file    *os.File // nil until allocated
	reading bool     // transitions at most once from false -> true
	length  int64    // length
}

func newNetSlurper(blob blob.Ref) *netSlurper {
	return &netSlurper{
		blob: blob,
		buf:  new(bytes.Buffer),
	}
}

func makeRestartable(br blob.Ref, r io.Reader, forceFile bool) (*netSlurper, error) {
	var err error
	if ns, ok := r.(*netSlurper); ok {
		if br != ns.blob {
			return ns, fmt.Errorf("the given netSlurper is for %q, you wanted %q ref", ns.blob, br)
		}
		if forceFile && ns.file == nil {
			err = ns.toFile()
		}
		return ns, err
	}
	b := newNetSlurper(br)
	if forceFile {
		if err = b.toFile(); err != nil {
			return nil, err
		}
	}
	_, err = io.Copy(b, r)
	return b, err
}

func (ns *netSlurper) Size() int64 {
	return ns.length
}

func (ns *netSlurper) Read(p []byte) (n int, err error) {
	if !ns.reading {
		ns.reading = true
		if ns.file != nil {
			ns.file.Seek(0, 0)
		}
	}
	//defer func() {
	//	log.Printf("Read: %d %s", n, err)
	//}()

	if ns.file != nil {
		return ns.file.Read(p)
	}
	return ns.buf.Read(p)
}

func (ns *netSlurper) Restart() error {
	var err error
	if ns.file == nil {
		if err = ns.toFile(); err != nil {
			return err
		}
	}
	_, err = ns.file.Seek(0, 0)
	return err
}

func (ns *netSlurper) Write(p []byte) (n int, err error) {
	if ns.reading {
		panic("write after read")
	}
	defer func() {
		ns.length += int64(n)
	}()
	if ns.file != nil {
		return ns.file.Write(p)
	}

	if ns.buf.Len()+len(p) > maxInMemorySlurp {
		if err = ns.toFile(); err != nil {
			return
		}
		return ns.file.Write(p)
	}
	return ns.buf.Write(p)
}

// switch from memory buffer to file
func (ns *netSlurper) toFile() (err error) {
	if ns.file, err = ioutil.TempFile("", ns.blob.String()); err != nil {
		return
	}
	if ns.buf == nil {
		return
	}
	if _, err = io.Copy(ns.file, ns.buf); err != nil {
		return
	}
	ns.buf = nil
	return nil
}

func (ns *netSlurper) Cleanup() {
	if ns.buf != nil {
		ns.buf.Reset()
	}
	if ns.file != nil {
		os.Remove(ns.file.Name())
	}
}

func (ns *netSlurper) Close() error {
	if ns.file != nil {
		ns.file.Close()
	}
	ns.Cleanup()
	return nil
}
