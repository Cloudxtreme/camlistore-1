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

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/blobserver/localdisk"
	"camlistore.org/pkg/cacher"
	"camlistore.org/pkg/client"
	"camlistore.org/pkg/fs"
	"camlistore.org/pkg/fs/fslib"
	"camlistore.org/pkg/types"

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

	f, err := fslib.NewRootedCamliFileSystem(cl, fetcher, br)
	if err != nil {
		return nil, closer, fmt.Errorf("initate filesystem: %v", err)
	}
	root := f.Root()
	if err != nil {
		return nil, closer, fmt.Errorf("get root: %v", err)
	}
	if root == nil {
		return nil, closer, fmt.Errorf("nil root!")
	}

	fi, err := root.Stat()
	if err != nil {
		return nil, closer, err
	}
	debug("root=%p (%T) fi=%#v", root, root, fi)
	debug("mode=%v", fi.Mode())
	if !fi.Mode().IsDir() {
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
	fslib.Node
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
	nd, ok := parent.(fslib.NodeDir)
	if !ok {
		log.Printf("Mkdir on non-dir %s", fs.Node)
		return os.ErrInvalid
	}
	_, err = nd.Mkdir(path.Base(name), perm)
	return err
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
		nd, ok := node.(fslib.NodeDir)
		if !ok {
			debug("Node of %s is not a NodeDir", path.Dir(name))
			return nil, os.ErrInvalid
		}
		subNode, err := nd.Create(path.Base(name), flag, perm)
		debug("Create(%s): %v", name, err)
		if err != nil {
			return nil, err
		}
		debug("Created %s.", name)
		subNodeFile, err := subNode.Open(flag)
		if err != nil {
			return nil, err
		}
		return &davFile{fs: fs, path: name, Node: subNodeFile}, nil
	}
	if fi, err := node.Stat(); err != nil {
		return nil, err
	} else if fi.Mode().IsDir() {
		debug("Open(%s): dir", name)
		return &davFile{fs: fs, path: name, Node: node}, nil
	}

	handle, err := node.Open(flag & ^os.O_CREATE)
	debug("Open(%s): %v", name, err)
	if err != nil {
		return nil, os.ErrNotExist
	}
	return &davFile{fs: fs, Node: handle, path: name}, nil
}

func (fs *davFS) RemoveAll(name string) error {
	name = strings.TrimSuffix(name, "/")
	debug("RemoveAll(%s)", name)
	node, err := fs.nodeForPath(name)
	if err != nil || node == nil {
		return nil
	}
	if _, isdir := node.(fslib.NodeDir); isdir {
		dirents, err := readdir(node)
		if err != nil {
			return err
		}
		debug("RemoveAll readdir(%s): %v", name, dirents)
		for _, dirent := range dirents {
			if err := fs.RemoveAll(path.Join(name, dirent.Name())); err != nil {
				return err
			}
		}
	}
	parent, err := fs.nodeForPath(path.Dir(name))
	if err != nil {
		return err
	}
	pd, ok := parent.(fslib.NodeDir)
	if !ok {
		debug("RemoveAll %s is not a NodeDir", path.Dir(name))
		return os.ErrInvalid
	}
	if err := pd.Remove(path.Base(name)); err != nil {
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
	oldDir, ok := node.(fslib.NodeDir)
	if !ok {
		debug("Rename %q is not a dir", od)
		return os.ErrInvalid
	}
	newDir := oldDir
	debug("Rename(%q, %q) od=%v nd=%s", oldName, newName, od, nd)
	if od != nd {
		newDirNode, err := fs.nodeForPath(nd)
		if err != nil {
			return err
		}
		if newDir, ok = newDirNode.(fslib.NodeDir); !ok {
			debug("Rename %q is not a dir", nd)
			return os.ErrInvalid
		}
	}
	return oldDir.Rename(path.Base(oldName), path.Base(newName), newDir)
}

func (fs *davFS) Stat(name string) (os.FileInfo, error) {
	name = strings.TrimSuffix(name, "/")
	debug("Stat(%s)", name)
	node, err := fs.nodeForPath(name)
	if err != nil {
		return nil, os.ErrNotExist
	}
	return node.Stat()
}

func readdir(node fslib.Node) ([]os.FileInfo, error) {
	debug("readdir(%s)", node)
	nd, ok := node.(fslib.NodeDir)
	if !ok {
		debug("readdir %s is not readable")
		return nil, os.ErrInvalid
	}

	dirents, err := nd.Readdir(-1)
	debug("ReadDir(%v): %v, %v", node, dirents, err)
	return dirents, err
}

func (fs *davFS) nodeForPath(p string) (fslib.Node, error) {
	node := fs.Node
	parts := strings.Split(strings.TrimPrefix(path.Clean(p), "/"), "/")
	var err error
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		nd, ok := node.(fslib.NodeDir)
		if !ok {
			debug("%s is not a NodeDir", node)
			return nil, os.ErrNotExist
		}
		if node, err = nd.Lookup(part); err != nil {
			return nil, err
		}
	}
	if node == nil {
		debug("NOT FOUND %q", p)
		return nil, os.ErrNotExist
	}
	return node, nil
}

var _ = webdav.File((*davFile)(nil))

type davFile struct {
	fs *davFS
	fslib.Node
	path string

	posDirentsMu sync.Mutex // protects pos, dirents
	pos          int64
	dirents      []os.FileInfo
	readdirErr   error
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

	if fi, err := f.Node.Stat(); err != nil {
		return nil, err
	} else if !fi.Mode().IsDir() {
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

	f.dirents = append(f.dirents, dirents...)
	return f.readdir(count)
}

func (f *davFile) Read(p []byte) (int, error) {
	debug("Reading %d bytes", len(p))
	if len(p) == 0 {
		return 0, nil
	}
	f.posDirentsMu.Lock()
	defer f.posDirentsMu.Unlock()
	nf, ok := f.Node.(fslib.NodeFile)
	if !ok {
		return 0, errors.New("not readable")
	}
	n, err := nf.ReadAt(p, f.pos)
	debug("Read %d bytes at %d", n, f.pos)
	f.pos += int64(n)
	return n, err
}

func (f *davFile) Write(p []byte) (n int, err error) {
	if len(p) == 0 {
		return 0, nil
	}
	debug("Writing %d bytes on %v", len(p), f)
	f.posDirentsMu.Lock()
	defer f.posDirentsMu.Unlock()
	nf, ok := f.Node.(fslib.NodeFile)
	if !ok {
		debug("Write %s not a NodeFile", f.Node)
		return 0, os.ErrInvalid
	}
	n, err = nf.WriteAt(p, f.pos)
	debug("Written %d bytes at %d", n, f.pos)
	f.pos += int64(n)
	return n, err
}

func (f *davFile) Seek(offset int64, whence int) (int64, error) {
	debug("Seek on %v", f)
	f.posDirentsMu.Lock()
	defer f.posDirentsMu.Unlock()
	pos := f.pos
	fi, err := f.Node.Stat()
	if err != nil {
		return 0, err
	}
	switch whence {
	case os.SEEK_SET:
		pos = 0
	case os.SEEK_END:
		pos = fi.Size()
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
	if cl, ok := f.Node.(io.Closer); ok {
		return cl.Close()
	}
	return nil
}

type removeDirCloser struct {
	dir string
}

func (rdc removeDirCloser) Close() error {
	return os.RemoveAll(rdc.dir)
}
