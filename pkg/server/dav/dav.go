// +build linux darwin

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
package dav

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/blobserver/localdisk"
	"camlistore.org/pkg/cacher"
	"camlistore.org/pkg/client"
	"camlistore.org/pkg/fs"
	"camlistore.org/pkg/types"

	"camlistore.org/third_party/bazil.org/fuse"
	fusefs "camlistore.org/third_party/bazil.org/fuse/fs"
	"camlistore.org/third_party/golang.org/x/net/webdav"
)

var (
	ErrMissingParent = errors.New("parent dir is missing")
)

// New returns a DAV server (http.Handler), which delivers all operations
// to a CamliFileSystem rooted at the blob referenced as rootRef.
func New(rootRef string, cl *client.Client) (http.Handler, io.Closer, error) {
	closer := types.NopCloser
	br, ok := blob.Parse(rootRef)
	if !ok {
		return nil, nil, fmt.Errorf("Invalid blobref %s", rootRef)
	}
	cacheDir, err := ioutil.TempDir("", "camlicache")
	if err != nil {
		return nil, nil, fmt.Errorf("Error creating temp cache directory: %v", err)
	}
	closer = removeDirCloser{cacheDir}
	diskcache, err := localdisk.New(cacheDir)
	if err != nil {
		return nil, closer, fmt.Errorf("Error setting up local disk cache: %v", err)
	}
	fetcher := cacher.NewCachingFetcher(diskcache, cl)

	f, err := fs.NewRootedCamliFileSystem(cl, fetcher, br)
	if err != nil {
		return nil, closer, fmt.Errorf("initate filesystem: %v", err)
	}
	root, err := f.Root()
	if err != nil {
		return nil, closer, fmt.Errorf("get root: %v", err)
	}

	if _, ok := root.(fusefs.NodeCreater); !ok {
		return nil, closer, fmt.Errorf("root should be a writable permanode, %s is read-only", br)
	}

	srv := &webdav.Handler{
		FileSystem: &davFS{Node: root},
		LockSystem: webdav.NewMemLS(),
		Logger: func(r *http.Request, err error) {
			debugPrn("WEBDAV ", "%s: %v", r, err)
		},
	}
	return srv, closer, nil
}

var dbg = new(int32)

func SetDebug(b bool) {
	var n int32
	if b {
		n = 1
	}
	o := atomic.LoadInt32(dbg)
	atomic.StoreInt32(dbg, n)
	if o != n {
		if b {
			fs.Debug = func(fmtString string, args ...interface{}) {
				debugPrn("FS ", fmtString, args...)
			}
		} else {
			fs.Debug = func(fmtString string, args ...interface{}) {}
		}
	}
}

func debug(fmt string, args ...interface{}) {
	debugPrn("CAMWEBDAV ", fmt, args...)
}

func debugPrn(prefix, fmt string, args ...interface{}) {
	if atomic.LoadInt32(dbg) != 0 {
		log.Printf(prefix+fmt, args...)
	}
}

var _ = webdav.FileSystem((*davFS)(nil))

type davFS struct {
	fusefs.Node
}

func (fs *davFS) Mkdir(name string, perm os.FileMode) error {
	name = strings.TrimSuffix(name, "/")
	debug("Mkdir(%s, %d)", name, perm)
	dir := path.Dir(name)
	if _, err := fs.nodeForPath(name); err == nil {
		return os.ErrExist
	}
	parent, err := fs.nodeForPath(dir)
	if err != nil {
		return os.ErrNotExist
	}
	md, ok := parent.(fusefs.NodeMkdirer)
	if !ok {
		debug("Node %s is not NodeMkdirer", dir)
		return os.ErrInvalid
	}
	if _, err = md.Mkdir(&fuse.MkdirRequest{
		Name: path.Base(name), Mode: perm | os.ModeDir,
	}, nil); err != nil {
		return err
	}
	return nil
}

func (fs *davFS) OpenFile(name string, flag int, perm os.FileMode) (webdav.File, error) {
	name = strings.TrimSuffix(name, "/")
	debug("OpenFile(%s, %d(create?%v), %d)", name, flag, flag&os.O_CREATE != 0, perm)
	node, err := fs.nodeForPath(name)
	debug("nodeForPath(%s): %s, %v", name, node, err)
	if err != nil {
		if flag&os.O_CREATE == 0 {
			debug("OpenFile(%s): no create, not exist", name)
			return nil, os.ErrNotExist
		}
		if flag&os.O_RDONLY != 0 {
			debug("OpenFile(%s): readonly, not exist", name)
			return nil, os.ErrNotExist
		}
		if node, err = fs.nodeForPath(path.Dir(name)); err != nil {
			debug("parent of %s: %v", name, err)
			return nil, os.ErrNotExist
		}
		var resp fuse.CreateResponse
		cr, ok := node.(fusefs.NodeCreater)
		if !ok {
			debug("Node of %s is not a NodeCreater", path.Dir(name))
			return nil, os.ErrInvalid
		}
		subNode, handle, err := cr.Create(
			&fuse.CreateRequest{
				Name:  path.Base(name),
				Flags: fuse.OpenFlags(flag),
				Mode:  perm,
			},
			&resp, nil)
		debug("Create(%s): %v", name, err)
		if err != nil {
			return nil, err
		}
		debug("Created %s.", name)
		return &davFile{fs: fs, Node: subNode, path: name, Handle: handle}, nil
	}
	if node.Attr().Mode.IsDir() {
		debug("Open(%s): dir", name)
		return &davFile{fs: fs, Node: node, path: name}, nil
	}

	var resp fuse.OpenResponse
	no, ok := node.(fusefs.NodeOpener)
	if !ok {
		debug("OpenFile(%s): response is not NodeOpener", name)
		return nil, os.ErrInvalid
	}
	handle, err := no.Open(
		&fuse.OpenRequest{Flags: fuse.OpenFlags(flag & (^os.O_CREATE))},
		&resp, nil)
	debug("Open(%s): %v", name, err)
	if err != nil {
		return nil, os.ErrNotExist
	}
	return &davFile{fs: fs, Node: node, Handle: handle, path: name}, nil
}

func (fs *davFS) RemoveAll(name string) error {
	name = strings.TrimSuffix(name, "/")
	debug("RemoveAll(%s)", name)
	node, err := fs.nodeForPath(name)
	if err != nil || node == nil {
		return nil
	}
	isdir := node.Attr().Mode.IsDir()
	if isdir {
		dirents, err := readdir(node)
		if err != nil {
			return err
		}
		debug("RemoveAll readdir(%s): %v", name, dirents)
		for _, dirent := range dirents {
			if err := fs.RemoveAll(path.Join(name, dirent.Name)); err != nil {
				return err
			}
		}
	}
	parent, err := fs.nodeForPath(path.Dir(name))
	if err != nil {
		return err
	}
	nr, ok := parent.(fusefs.NodeRemover)
	if !ok {
		debug("RemoveAll %s is not a NodeRemover", path.Dir(name))
		return os.ErrInvalid
	}
	if err := nr.Remove(
		&fuse.RemoveRequest{Name: path.Base(name), Dir: isdir}, nil); err != nil {
		return fmt.Errorf("Remove(%s): %v", name, err)
	}

	return nil
}

func (fs *davFS) Rename(oldName, newName string) error {
	oldName = strings.TrimSuffix(oldName, "/")
	newName = strings.TrimSuffix(newName, "/")
	debug("Rename(%s, %s)", oldName, newName)
	node, err := fs.nodeForPath(oldName)
	if err != nil {
		return err
	}
	if _, err = fs.nodeForPath(newName); err == nil {
		return os.ErrExist
	}
	od, nd := path.Dir(oldName), path.Dir(newName)
	if node, err = fs.nodeForPath(od); err != nil {
		return err
	}
	newDir := node
	debug("Rename(%q, %q) od=%v nd=%s", oldName, newName, od, nd)
	if od != nd {
		if newDir, err = fs.nodeForPath(nd); err != nil {
			return err
		}
	}
	if rnmr, ok := node.(fusefs.NodeRenamer); ok {
		return rnmr.Rename(&fuse.RenameRequest{
			OldName: path.Base(oldName), NewName: path.Base(newName)},
			newDir, nil)
	}
	debug("node %v is not renamer", node)
	return os.ErrInvalid
}

func (fs *davFS) Stat(name string) (os.FileInfo, error) {
	name = strings.TrimSuffix(name, "/")
	debug("Stat(%s)", name)
	node, err := fs.nodeForPath(name)
	if err != nil {
		return nil, os.ErrNotExist
	}
	return nodeFileInfo{Attr: node.Attr(), name: path.Base(name)}, nil
}

func readdir(node fusefs.Node) ([]fuse.Dirent, error) {
	debug("readdir(%s)", node)
	rdr, ok := node.(readDirer)
	if !ok {
		debug("readdir %s is not readable")
		return nil, os.ErrInvalid
	}

	dirents, err := rdr.ReadDir(nil)
	debug("ReadDir(%v): %v, %v", node, dirents, err)
	return dirents, err
}

var _ os.FileInfo = nodeFileInfo{}

type nodeFileInfo struct {
	fuse.Attr
	name string
}

func (nfi nodeFileInfo) Name() string       { return nfi.name }
func (nfi nodeFileInfo) Size() int64        { return int64(nfi.Attr.Size) }
func (nfi nodeFileInfo) Mode() os.FileMode  { return nfi.Attr.Mode }
func (nfi nodeFileInfo) ModTime() time.Time { return nfi.Attr.Mtime }
func (nfi nodeFileInfo) IsDir() bool        { return nfi.Attr.Mode.IsDir() }
func (nfi nodeFileInfo) Sys() interface{}   { return nil }

type readDirer interface {
	ReadDir(intr fusefs.Intr) ([]fuse.Dirent, fuse.Error)
}

func (fs *davFS) nodeForPath(p string) (fusefs.Node, error) {
	node := fs.Node
	parts := strings.Split(strings.TrimPrefix(path.Clean(p), "/"), "/")
	var err error
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		nl, ok := node.(fusefs.NodeStringLookuper)
		if !ok {
			debug("%s is not a NodeStringLookuper", node)
			return nil, os.ErrNotExist
		}
		if node, err = nl.Lookup(part, nil); err != nil {
			return nil, err
		}
	}
	if node == nil {
		debug("NOT FOUND %q", p)
		return nil, os.ErrNotExist
	}
	return node, nil
}

var _ webdav.File = (*davFile)(nil)

type davFile struct {
	fs *davFS
	fusefs.Node
	fusefs.Handle
	path string

	dirents      []os.FileInfo
	readdirErr   error
	pos          int64
	posDirentsMu sync.Mutex // protects pos, dirents
}

func (f davFile) Stat() (os.FileInfo, error) {
	return nodeFileInfo{name: path.Base(f.path), Attr: f.Node.Attr()}, nil
}

func (f *davFile) Readdir(count int) ([]os.FileInfo, error) {
	debug("Readdir(%s, %d)", f.path, count)
	f.posDirentsMu.Lock()
	defer f.posDirentsMu.Unlock()
	return f.readdir(count)
}

func (f *davFile) readdir(count int) ([]os.FileInfo, error) {
	if len(f.dirents) > 0 {
		if count <= 0 || count > len(f.dirents) {
			ret := f.dirents
			f.dirents = f.dirents[:0]
			return ret, f.readdirErr
		}
		var ret []os.FileInfo
		ret, f.dirents = f.dirents[:count], f.dirents[count:]
		return ret, f.readdirErr
	}

	if !f.Node.Attr().Mode.IsDir() {
		return nil, os.ErrInvalid
	}
	f.readdirErr = nil
	dirents, err := readdir(f.Node)
	// On error don't fail yet, fill up & return f.dirents first.
	if err != nil {
		f.readdirErr = err
	}
	if len(dirents) == 0 {
		return f.dirents[:0], err
	}

	if cap(f.dirents) < len(dirents) {
		f.dirents = make([]os.FileInfo, 0, len(dirents))
	}
	for _, dirent := range dirents {
		debug("Readdir nodeForPath(%q, %q)", f.path, dirent.Name)
		node, nfpErr := f.fs.nodeForPath(path.Join(f.path, dirent.Name))
		if nfpErr != nil {
			if err == nil {
				err = nfpErr
			}
			continue
		}
		f.dirents = append(f.dirents,
			nodeFileInfo{name: dirent.Name, Attr: node.Attr()})
	}
	return f.readdir(count)
}

func (f *davFile) Read(p []byte) (int, error) {
	debug("Reading %d bytes", len(p))
	if len(p) == 0 {
		return 0, nil
	}
	f.posDirentsMu.Lock()
	defer f.posDirentsMu.Unlock()
	rd, ok := f.Handle.(fusefs.HandleReader)
	if !ok {
		return 0, errors.New("not readable")
	}
	resp := new(fuse.ReadResponse)
	err := rd.Read(&fuse.ReadRequest{Offset: f.pos, Size: len(p)},
		resp, nil,
	)
	// FIXME(tgulacsi): this is against the spec, but fs does not return io.EOF!
	if len(resp.Data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, resp.Data)
	debug("Read %d bytes at %d", n, f.pos)
	f.pos += int64(n)
	return n, err
}

func (f *davFile) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	debug("Writing %d bytes on %v", len(p), f)
	var resp fuse.WriteResponse
	f.posDirentsMu.Lock()
	defer f.posDirentsMu.Unlock()
	hw, ok := f.Handle.(fusefs.HandleWriter)
	if !ok {
		debug("Write %s not a HandleWriter", f.Handle)
		return 0, os.ErrInvalid
	}
	err = hw.Write(
		&fuse.WriteRequest{
			Offset: f.pos,
			Data:   p,
		},
		&resp, nil)
	debug("Written %d bytes at %d", resp.Size, f.pos)
	f.pos += int64(resp.Size)
	return resp.Size, err
}

func (f *davFile) Seek(offset int64, whence int) (int64, error) {
	debug("Seek on %v", f)
	f.posDirentsMu.Lock()
	defer f.posDirentsMu.Unlock()
	pos := f.pos
	size := int64(f.Node.Attr().Size)
	switch whence {
	case os.SEEK_SET:
		pos = 0
	case os.SEEK_END:
		pos = size
	}
	if pos+offset < 0 {
		pos = f.pos
		return pos, errors.New("bad offset")
	}

	f.pos = pos + offset
	pos = f.pos
	return pos, nil
}

func (f *davFile) Close() error {
	if fl, ok := f.Handle.(fusefs.HandleFlusher); ok {
		debug("Flush(%v)", f)
		if err := fl.Flush(&fuse.FlushRequest{}, nil); err != nil {
			return err
		}
	}
	rel, ok := f.Handle.(fusefs.HandleReleaser)
	if !ok {
		return nil
	}
	return rel.Release(&fuse.ReleaseRequest{}, nil)
}

type removeDirCloser struct {
	dir string
}

func (rdc removeDirCloser) Close() error {
	return os.RemoveAll(rdc.dir)
}
