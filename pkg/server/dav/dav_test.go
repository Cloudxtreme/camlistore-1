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

package dav

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/client"
	"camlistore.org/pkg/osutil"
	"camlistore.org/pkg/test"
)

func TestBadRootRef(t *testing.T) {
	w := test.GetWorld(t)
	w.Start()
	defer w.Stop()

	dir, err := ioutil.TempDir("", "camlistore-test-")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(dir)
	out := test.MustRunCmd(t, w.Cmd("camput", "file", dir))
	br, ok := blob.Parse(strings.TrimSpace(out))
	if !ok {
		t.Fatalf("Expected permanode in camput stdout; got %q", out)
	}
	rootRef := br.String()
	t.Logf("root permanode: %s", rootRef)
	c, err := setupClient(w)
	if err != nil {
		t.Fatal("setupClient: %v", err)
	}

	// should error on non-writable blobref
	_, closer, err := New(rootRef, c)
	if err == nil {
		defer closer.Close()
		t.Fatal("New not fail on bad rootRef")
	}
	t.Logf("error for bad rootRef: %v", err)
}

func TestLitmus(t *testing.T) {
	cmd := exec.Command("docker", "build", "-t", "camlistore/litmus", "./testdata/")
	//cmd.Dir = filepath.Join(os.Getenv("GOPATH"), "src", "camlistore.org", "cmd", "camwebdav")
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	log.Printf("cmd=%#v", cmd)
	if err := cmd.Run(); err != nil {
		t.Skipf("docker build FAILED, skipping litmus test: %v", err)
	}

	ip := "172.17.42.1"
	cmd = exec.Command("ip", "addr", "show", "dev", "docker0")
	cmd.Stderr = os.Stderr
	out, err := cmd.Output()
	if err != nil {
		t.Logf("WARN cannot get ip addr of docker0: %v", err)
	} else {
		if i := bytes.Index(out, []byte("inet ")); i >= 0 {
			out = out[i+5:]
			if i = bytes.IndexByte(out, ' '); i >= 0 {
				out = out[:i]
			}
			if i = bytes.IndexByte(out, '/'); i >= 0 {
				out = out[:i]
			}
			ip = string(out)
		}
	}
	t.Logf("docker0 ip=%s", ip)

	w := test.GetWorld(t)
	if err := w.Start(); err != nil {
		t.Fatalf("cannot start World %v: %v", w, err)
	}
	defer w.Stop()

	pr := w.NewPermanode(t)
	rootRef := pr.String()
	t.Logf("root permanode: %s", rootRef)
	c, err := setupClient(w)
	if err != nil {
		t.Fatalf("setupClient: %v", err)
	}
	srv, closer, err := New(rootRef, c)
	if err != nil {
		t.Fatalf("New(%q): %v", rootRef, err)
	}
	defer closer.Close()
	http.Handle("/", srv)

	davaddr := ":8088"
	errCh := make(chan error, 1)
	go func(davaddr string) {
		log.Printf("Listening on %s...", davaddr)
		if err = http.ListenAndServe(davaddr, nil); err != nil {
			errCh <- err
		}
	}(davaddr)

	select {
	case err = <-errCh:
		t.Fatalf("Error starting DAV server: %v", err)
	case <-time.After(1 * time.Second): // started
	}

	// sometimes only the second/third attempt goes till the end
	for i := 0; i < 3; i++ {
		if i == 1 {
			SetDebug(true)
		}
		if err = testLitmus(t, "http://"+ip+davaddr); err == nil {
			break
		}
		t.Logf("Litmus: %v", err)
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		t.Error(err)
	}
}

func testLitmus(t *testing.T, url string) error {
	logf := t.Logf

	cmd := exec.Command("docker", "run", "-d", "camlistore/litmus", "make", "URL="+url, "check")
	out, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("docker run: %v", err)
	}
	cID := string(bytes.TrimSpace(out))

	cmd = exec.Command("docker", "attach", "--no-stdin", "--sig-proxy", cID)
	testOut, err := cmd.Output()
	logf("Litmus:\n%s", testOut)
	if err != nil && err.Error() != "exit status 2" {
		return fmt.Errorf("docker run: %v", err)
	}
	wd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("cannot get working directory: %v", err)
	}
	debugFn := filepath.Join(wd, "debug.log")
	if out, err = exec.Command("docker", "cp", cID+":/vapr/litmus/debug.log", wd).CombinedOutput(); err != nil {
		return fmt.Errorf("copying debug.log: %v\n%s", err, out)
	} else {
		logf("debug.log is available as %q", debugFn)
	}

	results := make(map[string]float64, 4)
	for _, line := range bytes.Split(testOut, []byte{'\n'}) {
		i := bytes.Index(line, []byte("<- summary for "))
		if i < 0 {
			continue
		}
		line = bytes.Replace(bytes.Replace(line[i+15:], []byte{'`'}, nil, 1), []byte{'\''}, nil, 1)
		if i = bytes.IndexByte(line, ':'); i < 0 {
			continue
		}
		name, line := line[:i], line[i+1:]
		if i = bytes.LastIndex(line, []byte{'%'}); i < 0 {
			continue
		}
		line = line[:i]
		if i = bytes.LastIndex(line, []byte(" failed. ")); i < 0 {
			continue
		}
		line = line[i+9:]
		percent, err := strconv.ParseFloat(string(line), 64)
		if err != nil {
			logf("Cannot parse percent from %q", line)
			continue
		}
		results[string(name)] = percent
	}

	logf("results: %v", results)

	// basic + copymove is 100%, props is only 71.4%.
	if results["props"] < 70 {
		return fmt.Errorf("props failed: %f%%", results["props"])
	}
	return nil
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
