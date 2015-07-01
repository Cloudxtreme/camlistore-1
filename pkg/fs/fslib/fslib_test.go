/*
Copyright 2015 The Camlistore Authors

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

package fslib

import (
	"bytes"
	"io"
	"os"
	"sync"
	"testing"

	"camlistore.org/pkg/client"
	"camlistore.org/pkg/osutil"
	"camlistore.org/pkg/test"
)

func TestFslib(t *testing.T) {
	w := test.GetWorld(t)
	if err := w.Start(); err != nil {
		t.Fatalf("cannot start World %v: %v", w, err)
	}
	defer w.Stop()

	pr := w.NewPermanode(t)
	rootRef := pr.String()
	t.Logf("root permanode: %s", rootRef)
	cl, err := setupClient(w)
	if err != nil {
		t.Fatalf("setupClient: %v", err)
	}

	fs, err := NewRootedCamliFileSystem(cl, cl, pr)
	if err != nil {
		t.Errorf("NewRootedCamliFileSystem: %v", err)
		t.FailNow()
	}
	root := fs.Root()
	_ = root.Name()

	fi, err := root.Stat()
	if err != nil {
		t.Fatalf("root.Stat: %v", err)
	}
	t.Logf("root=%p (%T) fi=%#v", root, root, fi)
	t.Logf("mode=%v", fi.Mode())
	if !fi.Mode().IsDir() {
		t.Fatalf("root should be a writable permanode, %s is read-only", root)
	}

	adir, err := root.(NodeDir).Mkdir("adir", 0755)
	if err != nil {
		t.Fatalf("cannot create adir: %v", err)
	}
	if _, err = adir.(NodeDir).Lookup("res"); err == nil {
		t.Fatalf("Found \"res\" in freshly created (empty) \"adir\"!")
	}
	res, err := adir.(NodeDir).Create("res", os.O_CREATE, 0600)
	if err != nil {
		t.Fatalf("cannot create adir/res: %v", err)
	}
	res.(NodeFile).Close()

	res, err = adir.(NodeDir).Lookup("res")
	if err != nil {
		t.Fatalf("Cannot lookup freshly created \"adir/res\": %v", err)
	}
	if fi, err = res.Stat(); err != nil {
		t.Fatalf("Error statting \"adir/res\": %v", err)
	}
	t.Logf("stat of adir/res: %#v", fi)
	h, err := res.Open(os.O_CREATE | os.O_RDWR)
	if err != nil {
		t.Fatalf("cannot open %s: %v", res.Name(), err)
	}
	content := []byte("Go is a nice concise simple language.")
	func() {
		defer func() {
			if err = h.(NodeFile).Close(); err != nil {
				t.Errorf("close of %s: %v", h, err)
			}
		}()
		if n, err := h.(NodeFile).WriteAt(content, 0); err != nil {
			t.Fatalf("write %s: %v", h, err)
		} else if n != len(content) {
			t.Errorf("written %d, awaited %d.", n, len(content))
		} else {
			t.Logf("Written %d bytes to %s.", n, h)
		}
	}()

	if res, err = adir.(NodeDir).Lookup("res"); err != nil {
		t.Fatalf("cannot found \"res\" in \"adir\": %v", err)
	}
	if h, err = res.Open(os.O_RDONLY); err != nil {
		t.Fatalf("cannot open %s (%s): %v", res.Name(), res, err)
	}
	func() {
		defer func() {
			if err = h.(NodeFile).Close(); err != nil {
				t.Errorf("close of %s: %v", h, err)
			}
		}()
		got := make([]byte, len(content)+1)
		if _, err := h.(NodeFile).ReadAt(got, 0); err != io.ErrUnexpectedEOF {
			t.Errorf("over read didn't erred out! (%v)", err)
		}
		got = got[:len(content)]
		if n, err := h.(NodeFile).ReadAt(got[:len(content)], 0); err != nil {
			t.Fatalf("reading %s: %v", h, err)
		} else if n != len(content) {
			t.Errorf("read %d, awaited %d bytes.", n, len(content))
		} else if !bytes.Equal(got, content) {
			t.Errorf("got %q, awaited %q.", got, content)
		}
	}()
}

var ringFlagOnce sync.Once

// copied from camlistore.org/pkg/importer/pinboard/pinboard_test.go
func setupClient(w *test.World) (*client.Client, error) {
	// Do the silly env vars dance to avoid the "non-hermetic use of host config panic".
	if err := os.Setenv("CAMLI_KEYID", w.ClientIdentity()); err != nil {
		return nil, err
	}
	if err := os.Setenv("CAMLI_SECRET_RING", w.SecretRingFile()); err != nil {
		return nil, err
	}
	ringFlagOnce.Do(osutil.AddSecretRingFlag)
	cl := client.New(w.ServerBaseURL())
	// This permanode is not needed in itself, but that takes care of uploading
	// behind the scenes the public key to the blob server. A bit gross, but
	// it's just for a test anyway.
	if _, err := cl.UploadNewPermanode(); err != nil {
		return nil, err
	}
	return cl, nil
}
