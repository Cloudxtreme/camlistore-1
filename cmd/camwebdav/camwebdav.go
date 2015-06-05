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

// The camwebdav binary is a WebDAV server to expose Camlistore as a
// filesystem that can be mounted from Windows (or other operating
// systems).
package main

import (
	"errors"
	"flag"
	"log"
	"net/http"

	"camlistore.org/pkg/client"
	"camlistore.org/pkg/fs/dav"
)

var (
	ErrMissingParent = errors.New("parent dir is missing")
)

// TODO(rh): tame copy/paste code from cammount
func main() {
	client.AddFlags()
	flagDAVAddr := flag.String("davaddr", ":8080", "WebDAV service address")
	flagDebug := flag.Bool("debug", false, "debug prints")
	flag.Parse()

	dav.SetDebug(*flagDebug)
	if flag.NArg() != 1 {
		log.Fatal("usage: camwebdav <blobref>")
	}
	srv, closer, err := dav.New(flag.Arg(0), client.NewOrFail())
	if err != nil {
		log.Fatalf("dav.New: %v", err)
	}
	defer func() {
		if err := closer.Close(); err != nil {
			log.Printf("ERROR closing DAV server: %v", err)
		}
	}()
	http.Handle("/", srv)

	log.Printf("Listening on %s...", *flagDAVAddr)
	if err := http.ListenAndServe(*flagDAVAddr, nil); err != nil {
		log.Fatalf("Error starting WebDAV server: %v", err)
	}
}
