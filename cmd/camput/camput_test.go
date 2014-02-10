/*
Copyright 2011 Google Inc.

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

package main

import (
	"bytes"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"

	"camlistore.org/pkg/cmdmain"
)

// env is the environment that a camput test runs within.
type env struct {
	// stdin is the standard input, or /dev/null if nil
	stdin io.Reader

	// TODO(bradfitz): vfs files.
}

func (e *env) Run(args ...string) (out, err []byte, exitCode int) {
	outbuf := new(bytes.Buffer)
	errbuf := new(bytes.Buffer)
	os.Args = append(os.Args[:1], args...)
	cmdmain.Stdout, cmdmain.Stderr = outbuf, errbuf
	exitc := make(chan int, 1)
	cmdmain.Exit = func(code int) {
		exitc <- code
		runtime.Goexit()
	}
	go func() {
		cmdmain.Main()
		cmdmain.Exit(0)
	}()
	select {
	case exitCode = <-exitc:
	case <-time.After(15 * time.Second):
		panic("timeout running command")
	}
	out = outbuf.Bytes()
	err = errbuf.Bytes()
	return
}

// TestUsageOnNoargs tests that we output a usage message when given no args, and return
// with a non-zero exit status.
func TestUsageOnNoargs(t *testing.T) {
	var e env
	out, err, code := e.Run()
	if code != 1 {
		t.Errorf("exit code = %d; want 1", code)
	}
	if len(out) != 0 {
		t.Errorf("wanted nothing on stdout; got:\n%s", out)
	}
	if !bytes.Contains(err, []byte("Usage: camput")) {
		t.Errorf("stderr doesn't contain usage. Got:\n%s", err)
	}
}

func TestUploadingChangingDirectory(t *testing.T) {
	// TODO(bradfitz):
	//    $ mkdir /tmp/somedir
	//    $ cp dev-camput /tmp/somedir
	//    $ ./dev-camput  -file /tmp/somedir/ 2>&1 | tee /tmp/somedir/log
	// ... verify it doesn't hang.
	t.Logf("TODO")
}

// Tests that uploads of deep directory trees don't deadlock.
// See commit ee4550bff453526ebae460da1ad59f6e7f3efe77 for backstory
func TestUploadDirectories(t *testing.T) {
	basedir, err := ioutil.TempDir("", "")
	if err != nil {
		t.Errorf("error creating temp dir: %v", err)
		return
	}
	defer os.RemoveAll(basedir)

	nm := basedir
	for i := 0; i < 20; i++ {
		for j := 0; j < 20; j++ {
			fn := filepath.Join(nm, strconv.Itoa(i)+"-"+strconv.Itoa(j))
			if j == 0 {
				if err = os.Mkdir(fn, 0700); err != nil {
					t.Errorf("error creating dir %s: %v", nm, err)
				}
			} else {
				if fh, err := os.Create(fn); err != nil {
					t.Errorf("error creating file %s: %v", nm, err)
				} else {
					if _, err := io.WriteString(fh, nm); err != nil {
						t.Errorf("error writing to %s: %v", nm, err)
					}
					fh.Close()
				}
			}
		}
		nm = filepath.Join(nm, strconv.Itoa(i)+"-0")
	}
	dir := filepath.Join(basedir, "0-0")
	if err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		t.Logf("%s %s", path, info.Mode())
		return nil
	}); err != nil {
		t.Errorf("error listing freshly created directory %s: %v", dir, err)
	}

	// local store, no need for real server
	*flagBlobDir = basedir

	uploadWorkers = 1
	donec := make(chan struct{})
	go func() {
		defer close(donec)
		tu := getUploader().NewTreeUpload(dir)
		tu.Start()
		lastPut, err := tu.Wait()
		if err != nil {
			t.Errorf("error uploading %s: %v", dir, err)
		}
		t.Logf("lastPut=%s", lastPut)
		// If we made it to the end, we didn't deadlock.
	}()

	select {
	case <-donec:
		t.Logf("Deeply nested directory tree upload finished in time (no deadlock).")
	case <-time.After(10 * time.Second):
		t.Errorf("Timeout (deadlock?) waiting for tree upload.")
	}
}
