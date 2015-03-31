// Copyright 2011 The Camlistore Authors.
//
// The camwebdav binary is a WebDAV server to expose Camlistore as a
// filesystem that can be mounted from Windows (or other operating
// systems).
package main

import (
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/blobserver/localdisk"
	"camlistore.org/pkg/cacher"
	"camlistore.org/pkg/client"
	"camlistore.org/pkg/fs"

	"camlistore.org/third_party/bazil.org/fuse"
	fusefs "camlistore.org/third_party/bazil.org/fuse/fs"
	"camlistore.org/third_party/github.com/gogits/webdav"
)

var davaddr = flag.String("davaddr", "", "WebDAV service address")

// TODO(rh): tame copy/paste code from cammount
func main() {
	client.AddFlags()
	flag.Parse()
	cacheDir, err := ioutil.TempDir("", "camlicache")
	if err != nil {
		log.Fatalf("Error creating temp cache directory: %v", err)
	}
	defer os.RemoveAll(cacheDir)
	diskcache, err := localdisk.New(cacheDir)
	if err != nil {
		log.Fatalf("Error setting up local disk cache: %v", err)
	}
	if flag.NArg() != 1 {
		log.Fatal("usage: camwebdav <blobref>")
	}
	br, ok := blob.Parse(flag.Arg(0))
	if !ok || !br.Valid() {
		log.Fatalf("%s was not a valid blobref.", flag.Arg(0))
	}
	client := client.NewOrFail()
	fetcher := cacher.NewCachingFetcher(diskcache, client)

	f, err := fs.NewRootedCamliFileSystem(client, fetcher, br)
	if err != nil {
		log.Fatalf("initate filesystem: %v", err)
	}
	root, err := f.Root()
	if err != nil {
		log.Fatalf("get root: %v", err)
	}

	http.Handle("/", &webdav.Server{Listings: true, Fs: davFS{root}})
	log.Printf("Listening on %s...", *davaddr)
	err = http.ListenAndServe(*davaddr, nil)
	if err != nil {
		log.Fatalf("Error starting WebDAV server: %v", err)
	}
}

var ErrNotFound = errors.New("not found")

var _ = webdav.FileSystem(davFS{})

type davFS struct {
	fusefs.Node
}

func (fs davFS) Open(name string) (webdav.File, error) {
	node, err := findPath(fs.Node, name)
	if err != nil {
		return nil, err
	}
	if node == nil {
		return nil, ErrNotFound
	}
	op, ok := node.(fusefs.NodeOpener)
	if !ok {
		log.Printf("%v not openable", node)
		return nil, fmt.Errorf("not openable %v", node)
	}
	resp := new(fuse.OpenResponse)
	handle, err := op.Open(
		&fuse.OpenRequest{Dir: node.Attr().Mode.IsDir()},
		resp, nil)
	if err != nil {
		return nil, err
	}
	return davFile{Handle: handle, Node: node, Name: baseName(name)}, nil
}
func (fs davFS) Create(name string) (webdav.File, error) {
	return nil, webdav.ErrNotImplemented
}
func (fs davFS) Mkdir(path string) error {
	return webdav.ErrNotImplemented
}
func (fs davFS) Remove(name string) error {
	return webdav.ErrNotImplemented
}

func findPath(root fusefs.Node, path string) (fusefs.Node, error) {
	parts := strings.Split(path, "/")
	log.Printf("findPath %v", parts)
	dir := root
	var err error
	for i, part := range parts {
		if part == "" {
			continue
		}
		if dir, err = dir.(fusefs.NodeStringLookuper).Lookup(part, nil); err != nil {
			log.Printf("cannot find %v: %v", parts[:i+1], err)
			return nil, err
		}
	}
	return dir, nil
}

var _ = webdav.File(davFile{})

type davFile struct {
	fusefs.Handle
	fusefs.Node
	Name string
	pos  int64
}

func (f davFile) Stat() (os.FileInfo, error) {
	return attrFileInfo{Attr: f.Node.Attr(), FileName: f.Name}, nil
}
func (f davFile) Readdir(count int) ([]os.FileInfo, error) {
	rd, ok := f.Handle.(fusefs.HandleReadDirer)
	if !ok {
		return nil, errors.New("not a dir")
	}
	dirents, err := rd.ReadDir(nil)
	if err != nil {
		return nil, err
	}
	fis := make([]os.FileInfo, len(dirents))
	for i, dent := range dirents {
		fis[i] = dentFileInfo{dent}
	}
	return fis, nil
}
func (f davFile) Read(p []byte) (int, error) {
	rd, ok := f.Handle.(fusefs.HandleReader)
	if !ok {
		return 0, errors.New("not readable")
	}
	resp := new(fuse.ReadResponse)
	if err := rd.Read(&fuse.ReadRequest{Offset: f.pos, Size: len(p)},
		resp, nil,
	); err != nil {
		return 0, err
	}
	copy(p, resp.Data)
	return len(resp.Data), nil
}
func (f davFile) Write(p []byte) (n int, err error) {
	return 0, webdav.ErrNotImplemented
}
func (f davFile) Seek(offset int64, whence int) (int64, error) {
	pos := f.pos
	size := int64(f.Node.Attr().Size)
	switch whence {
	case 0:
		pos = 0
	case 2:
		pos = size
	}
	if pos+offset < 0 || pos+offset > size {
		return f.pos, errors.New("bad offset")
	}

	f.pos = pos + offset
	return f.pos, nil
}
func (f davFile) Close() error {
	rel, ok := f.Handle.(fusefs.HandleReleaser)
	if !ok {
		return nil
	}
	return rel.Release(&fuse.ReleaseRequest{Dir: f.Attr().Mode.IsDir()}, nil)
}

var _ = os.FileInfo(attrFileInfo{})

type attrFileInfo struct {
	fuse.Attr
	FileName string
}

func (afi attrFileInfo) Name() string       { return afi.FileName }
func (afi attrFileInfo) Size() int64        { return int64(afi.Attr.Size) }
func (afi attrFileInfo) Mode() os.FileMode  { return afi.Attr.Mode }
func (afi attrFileInfo) ModTime() time.Time { return afi.Attr.Mtime }
func (afi attrFileInfo) IsDir() bool        { return afi.Attr.Mode.IsDir() }
func (afi attrFileInfo) Sys() interface{}   { return nil }

func baseName(name string) string {
	if i := strings.LastIndex(name, "/"); i >= 0 {
		return name[i+1:]
	}
	return name
}

var _ = os.FileInfo(dentFileInfo{})

type dentFileInfo struct {
	fuse.Dirent
}

func (dfi dentFileInfo) Name() string       { return dfi.Dirent.Name }
func (dfi dentFileInfo) Size() int64        { return 0 }
func (dfi dentFileInfo) Mode() os.FileMode  { return 0 }
func (dfi dentFileInfo) ModTime() time.Time { return time.Time{} }
func (dfi dentFileInfo) IsDir() bool        { return dfi.Dirent.Type&fuse.DT_Dir > 0 }
func (dfi dentFileInfo) Sys() interface{}   { return nil }
