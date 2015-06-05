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

// Package fslib provides helper methods for implementing filesystems foro Camlistore.
package fslib

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/client"
	"camlistore.org/pkg/lru"
	"camlistore.org/pkg/schema"
	"camlistore.org/pkg/types/camtypes"
)

var (
	Debug = func(fmt string, args ...interface{}) {}

	serverStart = time.Now()

	ErrNotDir         = errors.New("not a dir")
	ErrNoPerm         = errors.New("no permission")
	ErrNotImplemented = errors.New("not implemented")
)

type CamliFileSystem struct {
	fetcher blob.Fetcher
	client  *client.Client // may be nil for read-only fs
	root    Node

	// IgnoreOwners, if true, collapses all file ownership to the
	// uid/gid running the fuse filesystem, and sets all the
	// permissions to 0600/0700.
	IgnoreOwners bool

	blobToSchema *lru.Cache // ~map[blobstring]*schema.Blob
	nameToBlob   *lru.Cache // ~map[string]blob.Ref
	nameToAttr   *lru.Cache // ~map[string]*fuse.Attr
}

func (fs CamliFileSystem) Root() Node { return fs.root }

func NewCamliFileSystem(fetcher blob.Fetcher) *CamliFileSystem {
	return &CamliFileSystem{
		fetcher:      fetcher,
		blobToSchema: lru.New(1024), // arbitrary; TODO: tunable/smarter?
		nameToBlob:   lru.New(1024), // arbitrary: TODO: tunable/smarter?
		nameToAttr:   lru.New(1024), // arbitrary: TODO: tunable/smarter?
	}
}

// NewRootedCamliFileSystem returns a CamliFileSystem with a node based on a blobref
// as its base.
func NewRootedCamliFileSystem(cl *client.Client, fetcher blob.Fetcher, root blob.Ref) (*CamliFileSystem, error) {
	fs := NewCamliFileSystem(fetcher)
	fs.client = cl

	n, err := fs.newNodeFromBlobRef(root)
	if err != nil {
		return nil, err
	}
	fs.root = n
	log.Printf("fs=%p", fs)
	log.Printf("fs=%#v", fs)

	return fs, nil
}

type Node interface {
	Name() string
	Stat() (os.FileInfo, error)
	Open(int) (Node, error)
}
type NodeDir interface {
	Node
	Lookup(name string) (Node, error)
	Readdir(int) ([]os.FileInfo, error)
	Create(name string, flags int, mode os.FileMode) (Node, error)
	Mkdir(name string, mode os.FileMode) (Node, error)
	Remove(name string) error
	Rename(oldname, newname string, newDir Node) error
}

type NodeFile interface {
	Node
	io.ReaderAt
	io.WriterAt
	io.Closer
}

var _ = Node((*node)(nil))

// node implements Node with a read-only Camli "file" or
// "directory" blob.
type node struct {
	fs      *CamliFileSystem
	info    os.FileInfo
	blobref blob.Ref

	mu   sync.Mutex // guards below
	meta *schema.Blob

	pnodeModTime time.Time // optionally set by recent.go; modtime of permanode
}

func (n node) Name() string {
	if n.info == nil {
		return ""
	}
	return n.info.Name()
}
func (n *node) Stat() (os.FileInfo, error) {
	_, err := n.schema()
	log.Printf("&node{%p}.Stat: %#v (%v)", n, n.info, err)
	return n.info, err
}
func (n *node) Open(flags int) (Node, error) {
	Debug("CAMLI Open on %v: %v", n.blobref, flags)
	if isWriteFlags(flags) {
		return nil, ErrNoPerm
	}
	ss, err := n.schema()
	if err != nil {
		log.Printf("open of %v: %v", n.blobref, err)
		return nil, err
	}
	if ss.Type() == "directory" {
		return n, nil
	}
	fr, err := ss.NewFileReader(n.fs.fetcher)
	if err != nil {
		// Will only happen if ss.Type != "file" or "bytes"
		log.Printf("NewFileReader(%s) = %v", n.blobref, err)
		return nil, err
	}
	return nodeReader{FileReader: fr, node: *n}, nil
}
func (n *node) schema() (*schema.Blob, error) {
	// TODO: use singleflight library here instead of a lock?
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.meta == nil {
		blob, err := n.fs.fetchSchemaMeta(n.blobref)
		log.Printf("&node{%p}.schema: %#v (%v)", n, blob, err)
		if err != nil {
			return blob, err
		}
		n.meta = blob
	}
	if n.info == nil {
		n.populateAttr()
	}
	return n.meta, nil
}

var _ = NodeDir((*nodeDir)(nil))

type nodeDir struct {
	node
	dmu     sync.Mutex    // guards dirents. acquire before mu.
	dirents []os.FileInfo // nil until populated once

	mu      sync.Mutex // guards rest
	meta    *schema.Blob
	lookMap map[string]blob.Ref
}

func (n nodeDir) Create(string, int, os.FileMode) (Node, error) { return nil, os.ErrInvalid }
func (n nodeDir) Mkdir(string, os.FileMode) (Node, error)       { return nil, os.ErrInvalid }
func (n nodeDir) Stat() (os.FileInfo, error)                    { return n.node.Stat() }
func (n nodeDir) Open(int) (Node, error)                        { return n, nil }
func (n nodeDir) Remove(string) error                           { return os.ErrPermission }
func (n nodeDir) Rename(string, string, Node) error             { return os.ErrPermission }

func (n *nodeDir) addLookupEntry(name string, ref blob.Ref) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.lookMap == nil {
		n.lookMap = make(map[string]blob.Ref)
	}
	n.lookMap[name] = ref
}

func (n *nodeDir) Lookup(name string) (Node, error) {
	if name == ".quitquitquit" {
		// TODO: only in dev mode
		log.Fatalf("Shutting down due to .quitquitquit lookup.")
	}

	// If we haven't done Readdir yet (dirents isn't set), then force a Readdir
	// call to populate lookMap.
	n.dmu.Lock()
	loaded := n.dirents != nil
	n.dmu.Unlock()
	if !loaded {
		if _, err := n.Readdir(-1); err != nil {
			return nil, err
		}
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	ref, ok := n.lookMap[name]
	if !ok {
		log.Printf("no %q in %s (%v)", name, n, n.lookMap)
		return nil, os.ErrNotExist
	}
	return &node{fs: n.fs, blobref: ref}, nil
}

func isWriteFlags(flags int) bool {
	// TODO read/writeness are not flags, use O_ACCMODE
	return flags&(os.O_WRONLY|os.O_RDWR|os.O_APPEND|os.O_CREATE) != 0
}

func (n *nodeDir) Readdir(count int) ([]os.FileInfo, error) {
	Debug("CAMLI Readdir on %v", n.blobref)
	n.dmu.Lock()
	defer n.dmu.Unlock()
	if n.dirents != nil {
		return n.dirents, nil
	}

	ss, err := n.schema()
	if err != nil {
		log.Printf("camli.ReadDir error on %v: %v", n.blobref, err)
		return nil, err
	}
	dr, err := schema.NewDirReader(n.fs.fetcher, ss.BlobRef())
	if err != nil {
		log.Printf("camli.ReadDir error on %v: %v", n.blobref, err)
		return nil, err
	}
	schemaEnts, err := dr.Readdir(-1)
	if err != nil {
		log.Printf("camli.ReadDir error on %v: %v", n.blobref, err)
		return nil, err
	}
	n.dirents = make([]os.FileInfo, 0, len(schemaEnts))
	for _, sent := range schemaEnts {
		if name := sent.FileName(); name != "" {
			n.addLookupEntry(name, sent.BlobRef())
			n.dirents = append(n.dirents, FileInfo{name: name})
		}
	}
	return n.dirents, nil
}

var _ = Node((*nodeReader)(nil))
var _ = NodeFile((*nodeReader)(nil))

type nodeReader struct {
	*schema.FileReader
	node
}

func (nodeReader) WriteAt([]byte, int64) (int, error) { return 0, os.ErrInvalid }
func (nr nodeReader) Open(flags int) (Node, error)    { return nr.node.Open(flags) }
func (nr nodeReader) Stat() (os.FileInfo, error)      { return nr.node.Stat() }

var _ = os.FileInfo(FileInfo{})

type FileInfo struct {
	name  string
	size  int64
	mode  os.FileMode
	isDir bool
	mTime time.Time
}

func fileInfoFromBlob(blob *schema.Blob) FileInfo {
	info := FileInfo{name: blob.FileName(), mode: blob.FileMode()}
	switch blob.Type() {
	case "file":
		info.size = blob.PartsSize()
		info.mode |= 0400
	case "directory":
		info.mode |= 0500
		info.isDir = true
	case "symlink":
		info.mode |= 0400
	case "permanode":

	default:
		log.Printf("unknown attr ss.Type %q in fileInfoFromBlob", blob.Type())
	}
	info.mTime = blob.ModTime()
	return info
}

func (info FileInfo) Name() string       { return info.name }
func (info FileInfo) Size() int64        { return info.size }
func (info FileInfo) Mode() os.FileMode  { return info.mode }
func (info FileInfo) ModTime() time.Time { return info.mTime }
func (info FileInfo) IsDir() bool        { return info.isDir }
func (info FileInfo) Sys() interface{}   { return nil }

// populateAttr should only be called once n.ss is known to be set and
// non-nil
func (n *node) populateAttr() error {
	meta := n.meta

	info := fileInfoFromBlob(meta)
	if info.mTime.IsZero() {
		info.mTime = n.pnodeModTime
	}
	// TODO: inode?
	log.Printf("populateAttr(%s Type=%s): %#v (mode=%s)", n.Name(), meta.Type(), info, info.mode)

	n.info = info
	return nil
}

// Errors returned are:
//    os.ErrNotExist -- blob not found
//    os.ErrInvalid -- not JSON or a camli schema blob
func (fs *CamliFileSystem) fetchSchemaMeta(br blob.Ref) (*schema.Blob, error) {
	blobStr := br.String()
	if blob, ok := fs.blobToSchema.Get(blobStr); ok {
		return blob.(*schema.Blob), nil
	}

	rc, _, err := fs.fetcher.Fetch(br)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	blob, err := schema.BlobFromReader(br, rc)
	if err != nil {
		log.Printf("Error parsing %s as schema blob: %v", br, err)
		return nil, os.ErrInvalid
	}
	if blob.Type() == "" {
		log.Printf("blob %s is JSON but lacks camliType", br)
		return nil, os.ErrInvalid
	}
	fs.blobToSchema.Add(blobStr, blob)
	return blob, nil
}

// consolated logic for determining a node to mount based on an arbitrary blobref
func (fs *CamliFileSystem) newNodeFromBlobRef(root blob.Ref) (Node, error) {
	blob, err := fs.fetchSchemaMeta(root)
	if err != nil {
		return nil, err
	}

	switch blob.Type() {
	case "directory":
		n := &node{fs: fs, blobref: root, meta: blob}
		n.populateAttr()
		return n, nil

	case "permanode":
		// other mutDirs listed in the default fileystem have names and are displayed
		return &mutDir{node: node{fs: fs, blobref: root, meta: blob}}, nil
	}

	return nil, fmt.Errorf("Blobref must be of a directory or permanode got a %v", blob.Type())
}

func fileInfoFromFI(info camtypes.FileInfo, isDir bool) FileInfo {
	return FileInfo{
		name:  info.FileName,
		size:  info.Size,
		mode:  0640,
		isDir: isDir,
		mTime: time.Time(*info.ModTime),
	}
}
