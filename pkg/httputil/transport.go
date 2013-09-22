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

package httputil

import (
	"io"
	"log"
	"net/http"
	"sync"
	"time"

	"camlistore.org/pkg/tef"
)

// StatsTransport wraps another RoundTripper (or uses the default one) and
// counts the number of HTTP requests performed.
type StatsTransport struct {
	mu   sync.Mutex
	reqs int

	// Transport optionally specifies the transport to use.
	// If nil, http.DefaultTransport is used.
	Transport http.RoundTripper

	// If VerboseLog is true, HTTP request summaries are logged.
	VerboseLog bool

	// If create Stats is true, HTTP roundtrip times are logged.
	Stats chan<- []tef.Taskstats
}

func (t *StatsTransport) Requests() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.reqs
}

func (t *StatsTransport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	t.mu.Lock()
	t.reqs++
	n := t.reqs
	t.mu.Unlock()

	rt := t.Transport
	if rt == nil {
		rt = http.DefaultTransport
	}
	var t0 time.Time
	if t.VerboseLog || t.Stats != nil {
		t0 = time.Now()
		if t.VerboseLog {
			log.Printf("(%d) %s %s ...", n, req.Method, req.URL)
		}
	}
	resp, err = rt.RoundTrip(req)
	if t.VerboseLog || t.Stats != nil {
		t1 := time.Now()
		td := t1.Sub(t1)
		if t.VerboseLog {
			if err == nil {
				log.Printf("(%d) %s %s = status %d (in %v)", n, req.Method, req.URL, resp.StatusCode, td)
				resp.Body = &logBody{body: resp.Body, n: n, t0: t0, t1: t1}
			} else {
				log.Printf("(%d) %s %s = error: %v (in %v)", n, req.Method, req.URL, err, td)
			}
		}
		if t.Stats != nil {
            select {
            case t.Stats <- []tef.Taskstats{
				tef.Taskstats{Pid: uint32(t.reqs), Ppid: 1,
					Command:         req.Method + " " + req.URL.String(),
                    BlkioDelayTotal: uint64(td)}}:
                default:
                }
		}
	}
	return
}

type logBody struct {
	body      io.ReadCloser
	n         int
	t0, t1    time.Time
	readOnce  sync.Once
	closeOnce sync.Once
}

func (b *logBody) Read(p []byte) (n int, err error) {
	b.readOnce.Do(func() {
		log.Printf("(%d) Read body", b.n)
	})
	return b.body.Read(p)
}

func (b *logBody) Close() error {
	b.closeOnce.Do(func() {
		t := time.Now()
		log.Printf("(%d) Close body (%v tot, %v post-header)", b.n, t.Sub(b.t0), t.Sub(b.t1))
	})
	return b.body.Close()
}
