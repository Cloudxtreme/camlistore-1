/*
Copyright 2013 Camlistore Authors

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

package weedfs

import (
	"bytes"
	"io"
	"io/ioutil"
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
}

func newNetSlurper(blob blob.Ref) *netSlurper {
	return &netSlurper{
		blob: blob,
		buf:  new(bytes.Buffer),
	}
}

func (ns *netSlurper) Read(p []byte) (n int, err error) {
	if !ns.reading {
		ns.reading = true
		if ns.file != nil {
			ns.file.Seek(0, 0)
		}
	}
	if ns.file != nil {
		return ns.file.Read(p)
	}
	return ns.buf.Read(p)
}

func (ns *netSlurper) Write(p []byte) (n int, err error) {
	if ns.reading {
		panic("write after read")
	}
	if ns.file != nil {
		n, err = ns.file.Write(p)
		return
	}

	if ns.buf.Len()+len(p) > maxInMemorySlurp {
		ns.file, err = ioutil.TempFile("", ns.blob.String())
		if err != nil {
			return
		}
		_, err = io.Copy(ns.file, ns.buf)
		if err != nil {
			return
		}
		ns.buf = nil
		n, err = ns.file.Write(p)
		return
	}

	return ns.buf.Write(p)
}

func (ns *netSlurper) Cleanup() {
	if ns.file != nil {
		os.Remove(ns.file.Name())
	}
}

func (sto *weedfsStorage) ReceiveBlob(b blob.Ref, source io.Reader) (outsb blob.SizedRef, outerr error) {
	zero := outsb
	slurper := newNetSlurper(b)
	defer slurper.Cleanup()

	hash := b.Hash()
	size, err := io.Copy(io.MultiWriter(hash, slurper), source)
	if err != nil {
		return zero, err
	}
	err = sto.weedfsClient.Put(b.String(), size, slurper)
	if err != nil {
		return zero, err
	}
	return blob.SizedRef{Ref: b, Size: size}, nil
}
