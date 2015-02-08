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
	"bytes"
	"io/ioutil"
	"testing"
)

func TestCompress(t *testing.T) {
	var (
		cmpr, dcmpr []byte
		err         error
	)
	for i, ex := range []struct {
		src, dst []byte
	}{
		{[]byte(""), []byte("")},
		{[]byte("CaMlI"), []byte("CaMlI")},
		{[]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
			[]byte{255, 6, 0, 0, 115, 78, 97, 80, 112, 89, 0, 10, 0, 0, 30, 49, 130, 217, 42, 0, 97, 162, 1, 0}},
	} {
		if cmpr, err = compress(cmpr[:0], ex.src); err != nil {
			t.Errorf("%d. compress error: %v", i, err)
			continue
		}
		t.Logf("%d. compress(%q) length=%d", i, ex.src, len(cmpr))
		if !bytes.Equal(cmpr, ex.dst) {
			t.Errorf("%d. compress mismatch: got %x, awaited %x", i, cmpr, ex.dst)
			continue
		}
		dr := NewDecompressor(bytes.NewReader(cmpr))
		if dcmpr, err = ioutil.ReadAll(dr); err != nil {
			t.Errorf("%d. NewDecompressor error: %v", i, err)
			continue
		}
		t.Logf("%d. Decompress(%x)=[%d]%q", i, cmpr, len(dcmpr), dcmpr)
		if !bytes.Equal(dcmpr, ex.src) {
			t.Errorf("%d. decompress mismatch: got %q, awaited %q", i, dcmpr, ex.src)
			continue
		}
	}
}

func TestDecompress(t *testing.T) {
	var (
		raw []byte
		err error
	)
	for i, ex := range []struct {
		cmpr, raw []byte
	}{
		{[]byte(""), []byte("")},
		{[]byte("This is not a sNaPpY-compressed frame"),
			[]byte("This is not a sNaPpY-compressed frame")},
		{[]byte{255, 6, 0, 0, 115, 78, 97, 80, 112, 89, 0, 10, 0, 0, 30, 49, 130, 217, 42, 0, 97, 162, 1, 0},
			[]byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")},
	} {
		if raw, err = ioutil.ReadAll(NewDecompressor(bytes.NewReader(ex.cmpr))); err != nil {
			t.Errorf("%d. decompress error: %v", i, err)
			continue
		}
		if !bytes.Equal(raw, ex.raw) {
			t.Errorf("%d. decompress mismatch: got %x, awaited %x", i, raw, ex.raw)
			continue
		}
	}
}
