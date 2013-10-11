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
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"testing"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/blobserver"
	"camlistore.org/pkg/blobserver/storagetest"
	"camlistore.org/pkg/context"
	"camlistore.org/pkg/jsonconfig"
	"camlistore.org/pkg/test"
)

func newTempDiskpacked(t *testing.T) (sto blobserver.Storage, cleanup func()) {
	return newTempDiskpackedWithIndex(t, jsonconfig.Obj{})
}

func newTempDiskpackedMemory(t *testing.T) (sto blobserver.Storage, cleanup func()) {
	return newTempDiskpackedWithIndex(t, jsonconfig.Obj{
		"type": "memory",
	})
}

func newTempDiskpackedWithIndex(t *testing.T, indexConf jsonconfig.Obj) (sto blobserver.Storage, cleanup func()) {
	dir, err := ioutil.TempDir("", "diskpacked-test")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("diskpacked test dir is %q", dir)
	s, err := newStorage(dir, 1<<20, indexConf)
	if err != nil {
		t.Fatalf("newStorage: %v", err)
	}
	return s, func() {
		s.Close()

		testRemove(t, dir)

		if camliDebug {
			t.Logf("CAMLI_DEBUG set, skipping cleanup of dir %q", dir)
		} else {
			os.RemoveAll(dir)
		}
	}
}

func TestDiskpacked(t *testing.T) {
	storagetest.Test(t, newTempDiskpacked)
}

func TestDiskpackedAltIndex(t *testing.T) {
	storagetest.Test(t, newTempDiskpackedMemory)
}

func TestDoubleReceive(t *testing.T) {
	sto, cleanup := newTempDiskpacked(t)
	defer cleanup()

	size := func(n int) int64 {
		path := sto.(*storage).filename(n)
		fi, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		return fi.Size()
	}

	const blobSize = 5 << 10
	b := &test.Blob{Contents: strings.Repeat("a", blobSize)}
	br := b.BlobRef()

	_, err := blobserver.Receive(sto, br, b.Reader())
	if err != nil {
		t.Fatal(err)
	}
	if size(0) < blobSize {
		t.Fatalf("size = %d; want at least %d", size(0), blobSize)
	}
	sto.(*storage).nextPack()

	_, err = blobserver.Receive(sto, br, b.Reader())
	if err != nil {
		t.Fatal(err)
	}
	sizePostDup := size(1)
	if sizePostDup >= blobSize {
		t.Fatalf("size(pack1) = %d; appeared to double-write.", sizePostDup)
	}

	os.Remove(sto.(*storage).filename(0))
	_, err = blobserver.Receive(sto, br, b.Reader())
	if err != nil {
		t.Fatal(err)
	}
	sizePostDelete := size(1)
	if sizePostDelete < blobSize {
		t.Fatalf("after packfile delete + reupload, not big enough. want size of a blob")
	}
}

func testRemove(t *testing.T, dir string) {
	s, err := newStorage(dir, 1<<20, jsonconfig.Obj{})
	if err != nil {
		t.Fatalf("newStorage: %v", err)
	}
	var closeOnce sync.Once
	closeSto := func() { s.Close() }
	defer closeOnce.Do(closeSto)

	errch := make(chan error, 10)
	var errs []string
	go func() {
		for err := range errch {
			errs = append(errs, err.Error())
		}
	}()

	b1 := &test.Blob{"add one, to have remaining :)"}
	if _, err = s.ReceiveBlob(b1.BlobRef(), b1.Reader()); err != nil {
		t.Fatalf("ReceiveBlob of %s: %v", b1, err)
	}

	// remove all remaining
	sbc := make(chan blob.SizedRef, 10)
	go func() {
		if err := s.EnumerateBlobs(context.New(), sbc, "", 1000); err != nil {
			errch <- fmt.Errorf("EnumerateBlobs: %v", err)
		}
	}()
	var blobRefs []blob.Ref
	for sb := range sbc {
		blobRefs = append(blobRefs, sb.Ref)
	}
	t.Logf("enumeration blobRefs=%q errs=%q", blobRefs, errs)
	close(errch)
	if len(errs) > 0 {
		t.Errorf("error enumerating blobs: %s", errs)
	}
	if len(blobRefs) > 0 {
		if err := s.RemoveBlobs(blobRefs); err != nil {
			t.Errorf("error removing %v: %v", blobRefs)
			t.FailNow()
		}
	}
	closeOnce.Do(closeSto)

	ctx := context.TODO()

	// check deleteds
	n := 0
	if err = s.Walk(ctx,
		func(packID int, ref blob.Ref, offset int64, size uint32) error {
			//t.Logf("ref %q in %d at %d size=%d", ref, packId, offset, size)
			if size == 0 {
				t.Errorf("zero sized blob at %d in %d (%s)", offset, packID, ref)
			}
			if ref.Valid() {
				t.Errorf("valid blob remained at %d in %d (%s)", offset, packID, ref)
			} else {
				n++
			}
			return nil
		}); err != nil {
		t.Errorf("error walking %q: %v", s.root, err)
	}
	if n == 0 {
		t.Errorf("no deleted blob found in %q", s.root)
	} else {
		t.Logf("found %d deleted blobs in %q", n, s.root)
	}
}
