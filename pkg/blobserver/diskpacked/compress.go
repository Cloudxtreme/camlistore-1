/*
Copyright 2015 The Camlistore Authors.

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
	"io"
	"io/ioutil"

	"camlistore.org/third_party/github.com/mreiferson/go-snappystream"
)

// streamID is the stream identifier block that begins a valid snappy framed
// stream.
var streamID = []byte{0xff, 0x06, 0x00, 0x00, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59}

// compress src, iff it won't be bigger or is unclear: starts with "\xff\x06\x00\x00sNaPpY"
func compress(dst []byte, src []byte) ([]byte, error) {
	if len(src) == 0 {
		return dst[:0], nil
	}
	forceCompress := bytes.HasPrefix(src, streamID)
	buf := bytes.NewBuffer(dst[:0])
	sw := snappystream.NewWriter(buf)
	if _, err := sw.Write(src); err != nil {
		return nil, err
	}
	if !forceCompress && buf.Len() >= len(src) {
		dst = append(dst[:0], src...)
		return dst, nil
	}
	dst = buf.Bytes()
	return buf.Bytes(), nil
}

// compressFromStream reads r, and compresses it with compress.
func compressFromStream(r io.Reader) ([]byte, error) {
	src, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return compress(make([]byte, len(src)), src)
}

// NewDecompressor returns an io.Reader which decompresses transparently,
// iff the underlying Reader starts with streamID (snappy frame).
// Otherwise, it does nothing, just passes calls to the underlying Reader.
func NewDecompressor(r io.Reader) io.Reader {
	return &decompressor{r: r}
}

type decompressor struct {
	r     io.Reader
	began bool
}

func (d *decompressor) Read(p []byte) (int, error) {
	if d.began {
		return d.r.Read(p)
	}
	br := bufio.NewReader(d.r)
	d.r = br
	b, err := br.Peek(len(streamID))
	if err != nil && err != io.EOF {
		return 0, err
	}
	d.began = true
	if bytes.HasPrefix(b, streamID) {
		d.r = snappystream.NewReader(d.r, true)
	}
	return d.r.Read(p)
}

// NewDecomprAt returns an io.ReaderAt, which decompresses transparently.
func NewDecomprAt(r io.Reader) io.ReaderAt {
	b, err := ioutil.ReadAll(NewDecompressor(r))
	if err != nil {
		return errReaderAt{err}
	}
	return bytes.NewReader(b)
}

type errReaderAt struct {
	err error
}

func (r errReaderAt) ReadAt(_ []byte, _ int64) (int, error) {
	return 0, r.err
}
