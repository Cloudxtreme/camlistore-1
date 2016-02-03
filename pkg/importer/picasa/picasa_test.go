/*
Copyright 2014 The Camlistore Authors

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

package picasa

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"go4.org/ctxutil"
	"go4.org/syncutil"
	"golang.org/x/net/context"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/blobserver"
	"camlistore.org/pkg/httputil"
	"camlistore.org/pkg/importer"
	"camlistore.org/pkg/index"
	"camlistore.org/pkg/jsonsign"
	"camlistore.org/pkg/schema"
	"camlistore.org/pkg/search"
	"camlistore.org/pkg/server"
	"camlistore.org/pkg/sorted"
	"camlistore.org/pkg/test"

	"camlistore.org/third_party/github.com/tgulacsi/picago"
)

func TestGetUserId(t *testing.T) {
	userID := "11047045264"
	responder := httputil.FileResponder("testdata/users-me-res.xml")
	cl := &http.Client{
		Transport: httputil.NewFakeTransport(map[string]func() *http.Response{
			"https://picasaweb.google.com/data/feed/api/user/default/contacts?kind=user":        responder,
			"https://picasaweb.google.com/data/feed/api/user/" + userID + "/contacts?kind=user": responder,
		})}
	inf, err := picago.GetUser(cl, "default")
	if err != nil {
		t.Fatal(err)
	}
	want := picago.User{
		ID:        userID,
		URI:       "https://picasaweb.google.com/" + userID,
		Name:      "Tamás Gulácsi",
		Thumbnail: "https://lh4.googleusercontent.com/REDACTED/11047045264.jpg",
	}
	if inf != want {
		t.Errorf("user info = %+v; want %+v", inf, want)
	}
}

func TestMediaURLsEqual(t *testing.T) {
	if !mediaURLsEqual("https://lh1.googleusercontent.com/foo.jpg", "https://lh100.googleusercontent.com/foo.jpg") {
		t.Fatal("want equal")
	}
	if mediaURLsEqual("https://foo.com/foo.jpg", "https://bar.com/foo.jpg") {
		t.Fatal("want not equal")
	}
}

type testData struct {
	responses    map[string]func() *http.Response // make the response fail depending on which album is being imported
	isRunSuccess bool
	waitErr      error
}

type testResponse struct {
	ContErrors int
	errorCount int
	response   func() *http.Response
}

func (tr *testResponse) GetResponse() *http.Response {
	r := tr.response()
	log.Printf("testResponse CE=%d eC=%d", tr.ContErrors, tr.errorCount)
	if tr.ContErrors <= tr.errorCount {
		return r
	}
	tr.errorCount++
	r.StatusCode, r.Status = http.StatusInternalServerError, "500 Internal Server Error"
	return r
}

func testImportAlbums(t *testing.T, data testData) {
	h, err := newHost()
	if err != nil {
		t.Fatal(err)
	}
	testTopLevelNode, err = h.NewObject()
	if err != nil {
		t.Fatal(err)
	}
	cl := &http.Client{
		Transport: httputil.NewFakeTransport(data.responses),
	}
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), ctxutil.HTTPClient, cl), 10*time.Second)
	defer cancel()
	r := run{
		RunContext: &importer.RunContext{Context: ctx, Host: h},
		photoGate:  syncutil.NewGate(3),
	}

	if err := r.importAlbums(); err != nil && data.isRunSuccess || err != data.waitErr {
		t.Errorf("Should succeed? %t error=%v", data.isRunSuccess, err)
	}
}

var rJPGurl = regexp.MustCompile(`url="([^"]*[.](?:jpg|JPG))"`)

func TestImportAlbums(t *testing.T) {
	fis, err := ioutil.ReadDir("testdata")
	if err != nil {
		t.Fatal(err)
	}
	imgResp := httputil.FileResponder(filepath.Join("testdata", "img.jpg"))
	fixResponses := make(map[string]func() *http.Response, len(fis))
	var URL string
	var response func() *http.Response
	for _, fi := range fis {
		fn := fi.Name()
		if !(strings.HasPrefix(fn, "https") && strings.HasSuffix(fn, ".xml")) {
			continue
		}
		U, err := url.QueryUnescape(strings.TrimSuffix(fn, ".xml"))
		if err != nil {
			continue
		}
		fn = filepath.Join("testdata", fn)
		resp := httputil.FileResponder(fn)
		fixResponses[U] = resp
		if !strings.Contains(U, "/user/default?") && response == nil {
			response, URL = resp, U
		}

		b, err := ioutil.ReadFile(fn)
		if err != nil {
			t.Logf("Cannot read %q: %v", fn, err)
			continue
		}
		for _, m := range rJPGurl.FindAllSubmatch(b, -1) {
			fixResponses[string(m[1])] = imgResp
		}
	}
	//t.Logf("fix:\n%#v", fixResponses)
	fixAnd := func(ContErrors int) map[string]func() *http.Response {
		resp := make(map[string]func() *http.Response, len(fixResponses)+1)
		for k, v := range fixResponses {
			resp[k] = v
		}
		resp[URL] = (&testResponse{response: response, ContErrors: ContErrors}).GetResponse
		return resp
	}

	data := []testData{
		// 0 problem importing
		{responses: fixAnd(0), isRunSuccess: true},
		// at least 1 problem importing 1 album, but it succeeds after a few retries, so it's all good in the end
		{responses: fixAnd(1), isRunSuccess: true},
		// there's at least 1 problem importing 1 album, and it never succeeds, so we get a timeout for it. But we get less than maxContErrors, so we keep on importing until the end.
		{responses: fixAnd(3), isRunSuccess: false, waitErr: context.DeadlineExceeded},
		// there's enough problems that we go over maxContErrors
		{responses: fixAnd(100), isRunSuccess: false},
	}

	var wg sync.WaitGroup
	for i, v := range data {
		t.Logf("---- %d. ----", i)
		wg.Add(1)
		go func(v testData) {
			defer wg.Done()
			testImportAlbums(t, v)
		}(v)
	}
	wg.Wait()
}

func newSigner(bs blobserver.BlobReceiver) (signer *schema.Signer, owner blob.Ref, err error) {
	ent, err := jsonsign.NewEntity()
	if err != nil {
		return nil, owner, err
	}
	armorPub, err := jsonsign.ArmoredPublicKey(ent)
	if err != nil {
		return nil, owner, err
	}
	pubRef := blob.SHA1FromString(armorPub)
	if _, err := bs.ReceiveBlob(pubRef, strings.NewReader(armorPub)); err != nil {
		return nil, owner, fmt.Errorf("could not store pub key blob: %v", err)
	}
	sig, err := schema.NewSigner(pubRef, strings.NewReader(armorPub), ent)
	if err != nil {
		return nil, owner, err
	}
	return sig, pubRef, nil
}

func newHost() (*importer.Host, error) {
	bs := new(test.Fetcher)
	sig, owner, err := newSigner(bs)
	if err != nil {
		return nil, err
	}
	s := sorted.NewMemoryKeyValue()
	ix, err := index.New(s)
	if err != nil {
		return nil, err
	}
	ix.InitBlobSource(bs)
	corpus, err := ix.KeepInMemory()
	if err != nil {
		return nil, err
	}
	sh := search.NewHandler(ix, owner)
	sh.SetCorpus(corpus)

	server.NewSyncHandler("/bs/", "/index/", bs, ix, sorted.NewMemoryKeyValue())

	return importer.NewHost(importer.HostConfig{
		Prefix:     "picasa",
		Signer:     sig,
		Target:     bs,
		BlobSource: bs,
		Search:     sh,
	})
}
