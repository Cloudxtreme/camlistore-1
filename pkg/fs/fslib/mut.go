/*
Copyright 2013 Google Inc.

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
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/readerutil"
	"camlistore.org/pkg/schema"
	"camlistore.org/pkg/search"
	"camlistore.org/pkg/syncutil"
)

// How often to refresh directory nodes by reading from the blobstore.
const populateInterval = 30 * time.Second

// How long an item that was created locally will be present
// regardless of its presence in the indexing server.
const deletionRefreshWindow = time.Minute

type nodeType int

const (
	fileType nodeType = iota
	dirType
	symlinkType
)

var zeroTime time.Time

// mutDir is a mutable directory.
// Its br is the permanode with camliPath:entname attributes.
type mutDir struct {
	node
	parent *mutDir // or nil, if the root within its roots.go root.

	localCreateTime time.Time // time this node was created locally (iff it was)

	mu       sync.Mutex
	lastPop  time.Time
	children map[string]mutFileOrDir
	deleted  bool
}

func (m *mutDir) String() string {
	if m == nil {
		return "(*mutDir)(nil)"
	}
	return fmt.Sprintf("&mutDir{%p name=%q perm:%v}", m, m.fullPath(), m.node.blobref)
}

func (m *mutDir) Stat() (os.FileInfo, error) {
	// Use the same stat as the underlying node, but ensure we return a Dir.
	if m.info == nil || !m.info.Mode().IsDir() {
		_ = m.populate()
		fi, err := m.node.Stat()
		if err != nil {
			return fi, err
		}
		local := fi.(FileInfo)
		local.mode |= os.ModeDir
		m.node.info = local
	}
	return m.node.info, nil
}

// for debugging
func (n *mutDir) fullPath() string {
	if n == nil {
		return ""
	}
	if n.parent == nil {
		return n.Name()
	}
	return filepath.Join(n.parent.fullPath(), n.Name())
}

// populate hits the blobstore to populate map of child nodes.
func (n *mutDir) populate() error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Only re-populate if we haven't done so recently,
	// or something has written to it (through this lib).
	now := time.Now()
	if !n.lastPop.IsZero() && n.lastPop.Add(populateInterval).After(now) {
		return nil
	}
	n.lastPop = now

	res, err := n.fs.client.Describe(&search.DescribeRequest{
		BlobRef: n.blobref,
		Depth:   3,
	})
	if err != nil {
		log.Println("mutDir.paths:", err)
		return nil
	}
	db := res.Meta[n.blobref.String()]
	if db == nil {
		return errors.New("dir blobref not described")
	}

	// Find all child permanodes and stick them in n.children
	if n.children == nil {
		n.children = make(map[string]mutFileOrDir)
	}
	currentChildren := map[string]bool{}
	for k, v := range db.Permanode.Attr {
		const p = "camliPath:"
		if !strings.HasPrefix(k, p) || len(v) < 1 {
			continue
		}
		name := k[len(p):]
		childRef := v[0]
		child := res.Meta[childRef]
		if child == nil {
			log.Printf("child not described: %v", childRef)
			continue
		}
		if child.Permanode == nil {
			log.Printf("invalid child, not a permanode: %v", childRef)
			continue
		}
		if target := child.Permanode.Attr.Get("camliSymlinkTarget"); target != "" {
			// This is a symlink.
			_, fi, _ := child.PermanodeFile()
			var info os.FileInfo
			if fi != nil {
				info = fileInfoFromFI(*fi, false)
			}
			n.maybeAddChild(name, child.Permanode, &mutFile{
				node: &node{
					fs:      n.node.fs,
					blobref: blob.ParseOrZero(childRef),
					info:    info,
				},
				parent:  n,
				symLink: true,
				target:  target,
			})
		} else if isDir(child.Permanode) {
			// This is a directory.
			_, fi, _ := child.PermanodeDir()
			var info os.FileInfo
			if fi != nil {
				info = fileInfoFromFI(*fi, true)
			}
			n.maybeAddChild(name, child.Permanode, &mutDir{
				node: node{
					fs:      n.node.fs,
					blobref: blob.ParseOrZero(childRef),
					info:    info,
				},
				parent: n,
			})
		} else if contentRef := child.Permanode.Attr.Get("camliContent"); contentRef != "" {
			// This is a file.
			content := res.Meta[contentRef]
			if content == nil {
				log.Printf("child content not described: %v", childRef)
				continue
			}
			if content.CamliType != "file" {
				log.Printf("child not a file: %v", childRef)
				continue
			}
			if content.File == nil {
				log.Printf("camlitype \"file\" child %v has no described File member", childRef)
				continue
			}
			_, fi, _ := child.PermanodeFile()
			var info os.FileInfo
			if fi != nil {
				info = fileInfoFromFI(*fi, false)
			}
			n.maybeAddChild(name, child.Permanode, &mutFile{
				node: &node{
					fs:      n.node.fs,
					blobref: blob.ParseOrZero(childRef),
					info:    info,
				},
				parent:  n,
				content: blob.ParseOrZero(contentRef),
				size:    content.File.Size,
			})
		} else {
			// unhandled type...
			continue
		}
		currentChildren[name] = true
	}
	// Remove unreferenced children
	for name, oldchild := range n.children {
		if _, ok := currentChildren[name]; !ok {
			if oldchild.eligibleToDelete() {
				delete(n.children, name)
			}
		}
	}
	return nil
}

// maybeAddChild adds a child directory to this mutable directory
// unless it already has one with this name and permanode.
func (m *mutDir) maybeAddChild(name string, permanode *search.DescribedPermanode,
	child mutFileOrDir,
) {
	if current, ok := m.children[name]; !ok ||
		current.permanodeString() != child.permanodeString() {

		m.children[name] = child
	}
}

func isDir(d *search.DescribedPermanode) bool {
	// Explicit
	if d.Attr.Get("camliNodeType") == "directory" {
		return true
	}
	// Implied
	for k := range d.Attr {
		if strings.HasPrefix(k, "camliPath:") {
			return true
		}
	}
	return false
}

func (n *mutDir) Readdir(count int) ([]os.FileInfo, error) {
	if err := n.populate(); err != nil {
		log.Println("populate:", err)
		return nil, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	var ents []os.FileInfo
	for name, childNode := range n.children {
		var dirent os.FileInfo
		var err error
		switch v := childNode.(type) {
		case *mutDir:
			dirent, err = v.Stat()
		case *mutFile:
			dirent, err = v.Stat()
		default:
			log.Printf("mutDir.ReadDir: unknown child %q (%T)", name, childNode)
		}
		if err != nil {
			return ents, err
		}

		// TODO: figure out what Dirent.Type means.
		Debug("mutDir(%q) appending %+v", n.fullPath(), dirent)
		ents = append(ents, dirent)
	}
	return ents, nil
}

func (n *mutDir) Lookup(name string) (ret Node, err error) {
	defer func() {
		Debug("mutDir(%q).Lookup(%q) = %v, %v", n.fullPath(), name, ret, err)
	}()
	if err := n.populate(); err != nil {
		log.Println("populate:", err)
		return nil, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	if n2 := n.children[name]; n2 != nil {
		return n2, nil
	}
	return nil, os.ErrNotExist
}

// Create of regular file. (not a dir)
//
// Flags are always 514:  O_CREAT is 0x200 | O_RDWR is 0x2.
// From fuse_vnops.c:
//    /* XXX: We /always/ creat() like this. Wish we were on Linux. */
//    foi->flags = O_CREAT | O_RDWR;
//
// 2013/07/21 05:26:35 <- &{Create [ID=0x3 Node=0x8 Uid=61652 Gid=5000 Pid=13115] "x" fl=514 mode=-rw-r--r-- fuse.Intr}
// 2013/07/21 05:26:36 -> 0x3 Create {LookupResponse:{Node:23 Generation:0 EntryValid:1m0s AttrValid:1m0s Attr:{Inode:15976986887557313215 Size:0 Blocks:0 Atime:2013-07-21 05:23:51.537251251 +1200 NZST Mtime:2013-07-21 05:23:51.537251251 +1200 NZST Ctime:2013-07-21 05:23:51.537251251 +1200 NZST Crtime:2013-07-21 05:23:51.537251251 +1200 NZST Mode:-rw------- Nlink:1 Uid:61652 Gid:5000 Rdev:0 Flags:0}} OpenResponse:{Handle:1 Flags:0}}
func (n *mutDir) Create(name string, flags int, mode os.FileMode) (Node, error) {
	child, err := n.creat(name, fileType)
	if err != nil {
		log.Printf("mutDir.Create(%q): %v", name, err)
		return nil, err
	}

	// Create and return a file handle.
	h, ferr := child.(*mutFile).newHandle(nil)
	if ferr != nil {
		return nil, ferr
	}

	return h, nil
}

func (n *mutDir) Mkdir(name string, mode os.FileMode) (Node, error) {
	child, err := n.creat(name, dirType)
	if err != nil {
		log.Printf("mutDir.Mkdir(%q): %v", name, err)
		return nil, err
	}
	return child, nil
}

// &fuse.SymlinkRequest{Header:fuse.Header{Conn:(*fuse.Conn)(0xc210047180), ID:0x4, Node:0x8, Uid:0xf0d4, Gid:0x1388, Pid:0x7e88}, NewName:"some-link", Target:"../../some-target"}
func (n *mutDir) Symlink(oldname, newname string) (Node, error) {
	node, err := n.creat(newname, symlinkType)
	if err != nil {
		log.Printf("mutDir.Symlink(%q): %v", newname, err)
		return nil, err
	}
	mf := node.(*mutFile)
	mf.symLink = true
	mf.target = newname

	claim := schema.NewSetAttributeClaim(mf.blobref, "camliSymlinkTarget", oldname)
	_, err = n.fs.client.UploadAndSignBlob(claim)
	if err != nil {
		log.Printf("mutDir.Symlink(%q) upload error: %v", newname, err)
		return nil, err
	}

	return node, nil
}

func (n *mutDir) creat(name string, typ nodeType) (Node, error) {
	// Create a Permanode for the file/directory.
	pr, err := n.fs.client.UploadNewPermanode()
	if err != nil {
		return nil, err
	}

	var grp syncutil.Group
	grp.Go(func() (err error) {
		// Add a camliPath:name attribute to the directory permanode.
		claim := schema.NewSetAttributeClaim(n.blobref, "camliPath:"+name, pr.BlobRef.String())
		_, err = n.fs.client.UploadAndSignBlob(claim)
		return
	})

	// Hide OS X Finder .DS_Store junk.  This is distinct from
	// extended attributes.
	if name == ".DS_Store" {
		grp.Go(func() (err error) {
			claim := schema.NewSetAttributeClaim(pr.BlobRef, "camliDefVis", "hide")
			_, err = n.fs.client.UploadAndSignBlob(claim)
			return
		})
	}

	if typ == dirType {
		grp.Go(func() (err error) {
			// Set a directory type on the permanode
			claim := schema.NewSetAttributeClaim(pr.BlobRef, "camliNodeType", "directory")
			_, err = n.fs.client.UploadAndSignBlob(claim)
			return
		})
		grp.Go(func() (err error) {
			// Set the permanode title to the directory name
			claim := schema.NewSetAttributeClaim(pr.BlobRef, "title", name)
			_, err = n.fs.client.UploadAndSignBlob(claim)
			return
		})
	}
	if err := grp.Err(); err != nil {
		return nil, err
	}

	// Add a child node to this node.
	var child mutFileOrDir
	switch typ {
	case dirType:
		child = &mutDir{
			node: node{
				fs:      n.node.fs,
				blobref: pr.BlobRef,
				info:    FileInfo{name: name, mode: os.ModeDir | 0666},
			},
			parent:          n,
			localCreateTime: time.Now(),
		}
	case fileType, symlinkType:
		child = &mutFile{
			node: &node{
				fs:      n.node.fs,
				blobref: pr.BlobRef,
				info:    FileInfo{name: name, mode: 0666},
			},
			parent:          n,
			localCreateTime: time.Now(),
		}
	default:
		panic("bogus creat type")
	}
	n.mu.Lock()
	if n.children == nil {
		n.children = make(map[string]mutFileOrDir)
	}
	n.children[name] = child
	n.lastPop = zeroTime
	n.mu.Unlock()

	Debug("Created %v in %p", child, n)

	return child, nil
}

func (n *mutDir) Remove(name string) error {
	// Remove the camliPath:name attribute from the directory permanode.
	claim := schema.NewDelAttributeClaim(n.blobref, "camliPath:"+name, "")
	_, err := n.fs.client.UploadAndSignBlob(claim)
	if err != nil {
		log.Println("mutDir.Remove:", err)
		return err
	}
	// Remove child from map.
	n.mu.Lock()
	if n.children != nil {
		if removed, ok := n.children[name]; ok {
			removed.invalidate()
			delete(n.children, name)
			Debug("Removed %v from %p", removed, n)
		}
	}
	n.lastPop = zeroTime
	n.mu.Unlock()
	return nil
}

// &RenameRequest{Header:fuse.Header{Conn:(*fuse.Conn)(0xc210048180), ID:0x2, Node:0x8, Uid:0xf0d4, Gid:0x1388, Pid:0x5edb}, NewDir:0x8, OldName:"1", NewName:"2"}
func (n *mutDir) Rename(oldname, newname string, newDir Node) error {
	n2, ok := newDir.(*mutDir)
	if !ok {
		log.Printf("*mutDir newDir node isn't a *mutDir; is a %T; can't handle. returning EIO.", newDir)
		return fmt.Errorf("*mutDir newDir node isn't a *mutDir; is a %T; can't handle. returning EIO.", newDir)
	}

	var wg syncutil.Group
	wg.Go(n.populate)
	wg.Go(n2.populate)
	if err := wg.Err(); err != nil {
		log.Printf("*mutDir.Rename src dir populate = %v", err)
		return err
	}

	n.mu.Lock()
	target, ok := n.children[oldname]
	n.mu.Unlock()
	if !ok {
		log.Printf("*mutDir.Rename src name %q isn't known", oldname)
		return fmt.Errorf("*mutDir.Rename src name %q isn't known", oldname)
	}

	now := time.Now()

	// Add a camliPath:name attribute to the dest permanode before unlinking it from
	// the source.
	claim := schema.NewSetAttributeClaim(n2.blobref, "camliPath:"+newname, target.permanodeString())
	claim.SetClaimDate(now)
	_, err := n.fs.client.UploadAndSignBlob(claim)
	if err != nil {
		log.Printf("Upload rename link error: %v", err)
		return fmt.Errorf("Upload rename link error: %v", err)
	}

	var grp syncutil.Group
	// Unlink the dest permanode from the source.
	grp.Go(func() (err error) {
		delClaim := schema.NewDelAttributeClaim(n.blobref, "camliPath:"+oldname, "")
		delClaim.SetClaimDate(now)
		_, err = n.fs.client.UploadAndSignBlob(delClaim)
		return
	})
	// If target is a directory then update its title.
	if dir, ok := target.(*mutDir); ok {
		grp.Go(func() (err error) {
			claim := schema.NewSetAttributeClaim(dir.blobref, "title", newname)
			_, err = n.fs.client.UploadAndSignBlob(claim)
			return
		})
	}
	if err := grp.Err(); err != nil {
		log.Printf("Upload rename unlink/title error: %v", err)
		return err
	}

	// TODO(bradfitz): this locking would be racy, if the kernel
	// doesn't do it properly. (It should) Let's just trust the
	// kernel for now. Later we can verify and remove this
	// comment.
	n.mu.Lock()
	if n.children[oldname] != target {
		panic("Race.")
	}
	delete(n.children, oldname)
	n.lastPop = zeroTime
	n.mu.Unlock()
	n2.mu.Lock()
	n2.children[newname] = target
	n2.lastPop = zeroTime
	n2.mu.Unlock()

	return nil
}

// mutFile is a mutable file, or symlink.
type mutFile struct {
	*node
	parent *mutDir

	localCreateTime time.Time // time this node was created locally (iff it was)

	mu      sync.Mutex // protects all following fields
	size    int64
	symLink bool     // if true, is a symlink
	target  string   // if a symlink
	content blob.Ref // if a regular file
	deleted bool
}

func (m *mutFile) String() string {
	if m == nil {
		return "(*mutFile)(nil)"
	}
	return fmt.Sprintf("&mutFile{%p name=%q perm:%v}", m, m.fullPath(), m.blobref)
}
func (m mutFile) ReadAt([]byte, int64) (int, error)  { return 0, os.ErrInvalid }
func (m mutFile) WriteAt([]byte, int64) (int, error) { return 0, os.ErrInvalid }

// for debugging
func (n *mutFile) fullPath() string {
	if n == nil {
		return ""
	}
	if n.parent == nil {
		return n.Name()
	}
	return filepath.Join(n.parent.fullPath(), n.Name())
}

func (n *mutFile) Stat() (os.FileInfo, error) {
	n.mu.Lock()
	info, err := n.node.Stat()
	n.mu.Unlock()
	if err != nil {
		return info, err
	}
	fi := info.(FileInfo)
	fi.size = n.size
	if info.ModTime().IsZero() {
		fi.mTime = serverStart
	}
	return fi, nil
}

func (n *mutFile) setContent(br blob.Ref, size int64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.content = br
	n.size = size
	claim := schema.NewSetAttributeClaim(n.blobref, "camliContent", br.String())
	_, err := n.fs.client.UploadAndSignBlob(claim)
	n.meta = nil
	return err
}

func (n *mutFile) setSizeAtLeast(size int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	Debug("mutFile.setSizeAtLeast(%d). old size = %d", size, n.size)
	if size > n.size {
		n.size = size
	}
}

// Empirically:
//  open for read:   req.Flags == 0
//  open for append: req.Flags == 1
//  open for write:  req.Flags == 1
//  open for read/write (+<)   == 2 (bitmask? of?)
//
// open flags are O_WRONLY (1), O_RDONLY (0), or O_RDWR (2). and also
// bitmaks of O_SYMLINK (0x200000) maybe. (from
// fuse_filehandle_xlate_to_oflags in macosx/kext/fuse_file.h)
func (n *mutFile) Open(flags int) (Node, error) {
	Debug("mutFile.Open: %v: content: %v flags=%v", n.blobref, n.content, flags)
	r, err := schema.NewFileReader(n.fs.fetcher, n.content)
	if err != nil {
		log.Printf("mutFile.Open: %v", err)
		return nil, err
	}
	// Read-only.
	if !isWriteFlags(flags) {
		return &nodeReader{FileReader: r, node: n.node}, nil
	}
	Debug("mutFile.Open returning read-write filehandle")

	defer r.Close()
	return n.newHandle(r)
}

func (n *mutFile) Sync() error {
	log.Printf("mutFile.Sync: TODO")
	return nil
}

func (n *mutFile) Readlink() (string, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if !n.symLink {
		log.Printf("mutFile.Readlink on node that's not a symlink?")
		return "", fmt.Errorf("mutFile.Readlink on node that's not a symlink?")
	}
	return n.target, nil
}

func (n *mutFile) newHandle(body io.Reader) (Node, error) {
	tmp, err := ioutil.TempFile("", "camli-")
	if err == nil && body != nil {
		_, err = io.Copy(tmp, body)
	}
	if err != nil {
		log.Printf("mutFile.newHandle: %v", err)
		if tmp != nil {
			tmp.Close()
			os.Remove(tmp.Name())
		}
		return nil, err
	}
	return &mutFileHandle{f: n, tmp: tmp}, nil
}

// mutFileHandle represents an open mutable file.
// It stores the file contents in a temporary file, and
// delegates reads and writes directly to the temporary file.
// When the handle is released, it writes the contents of the
// temporary file to the blobstore, and instructs the parent
// mutFile to update the file permanode.
type mutFileHandle struct {
	f   *mutFile
	tmp *os.File
}

func (h *mutFileHandle) Open(int) (Node, error) { return h, nil }
func (h *mutFileHandle) String() string {
	if h == nil {
		return "(*mutFileHandle)(nil)"
	}
	if h.f == nil {
		return fmt.Sprintf("&mutFileHandle{%p}", h)
	}
	return fmt.Sprintf("&mutFileHandle{%p name=%q perm:%v}", h, h.f.fullPath(), h.f.blobref)
}
func (h mutFileHandle) Name() string                       { return h.f.Name() }
func (h mutFileHandle) Readdir(int) ([]os.FileInfo, error) { return nil, os.ErrInvalid }
func (h mutFileHandle) Stat() (os.FileInfo, error)         { return h.f.Stat() }

func (h *mutFileHandle) ReadAt(p []byte, offset int64) (int, error) {
	if h.tmp == nil {
		log.Printf("Read called on camli mutFileHandle without a tempfile set")
		return 0, fmt.Errorf("Read called on camli mutFileHandle without a tempfile set")
	}

	return h.tmp.ReadAt(p, offset)
}

func (h *mutFileHandle) WriteAt(p []byte, offset int64) (int, error) {
	if h.tmp == nil {
		log.Printf("Write called on camli mutFileHandle without a tempfile set")
		return 0, fmt.Errorf("Write called on camli mutFileHandle without a tempfile set")
	}

	n, err := h.tmp.WriteAt(p, offset)
	h.f.setSizeAtLeast(int64(n) + offset)
	return n, err
}

// Flush is called to let the file system clean up any data buffers
// and to pass any errors in the process of closing a file to the user
// application.
//
// Flush *may* be called more than once in the case where a file is
// opened more than once, but it's not possible to detect from the
// call itself whether this is a final flush.
//
// This is generally the last opportunity to finalize data and the
// return value sets the return value of the Close that led to the
// calling of Flush.
//
// Note that this is distinct from Fsync -- which is a user-requested
// flush (fsync, etc...)
func (h *mutFileHandle) Flush() error {
	if h.tmp == nil {
		log.Printf("Flush called on camli mutFileHandle without a tempfile set")
		return fmt.Errorf("Flush called on camli mutFileHandle without a tempfile set")
	}
	_, err := h.tmp.Seek(0, 0)
	if err != nil {
		log.Println("mutFileHandle.Flush:", err)
		return err
	}
	var n int64
	br, err := schema.WriteFileFromReader(h.f.fs.client, h.f.Name(), readerutil.CountingReader{Reader: h.tmp, N: &n})
	if err != nil {
		log.Println("mutFileHandle.Flush:", err)
		return err
	}
	err = h.f.setContent(br, n)
	if err != nil {
		log.Printf("mutFileHandle.Flush: %v", err)
		return err
	}

	return nil
}

// Close is called when a file handle is no longer needed.  This is
// called asynchronously after the last handle to a file is closed.
func (h *mutFileHandle) Close() error {
	err := h.Flush()
	_ = h.tmp.Close()
	_ = os.Remove(h.tmp.Name())
	h.tmp = nil

	return err
}

func (h *mutFileHandle) Truncate(size int64) error {
	if h.tmp == nil {
		log.Printf("Truncate called on camli mutFileHandle without a tempfile set")
		return fmt.Errorf("Truncate called on camli mutFileHandle without a tempfile set")
	}

	Debug("mutFileHandle.Truncate(%q) to size %d", h.f.fullPath(), size)
	if err := h.tmp.Truncate(size); err != nil {
		log.Println("mutFileHandle.Truncate:", err)
		return err
	}
	return nil
}

// mutFileOrDir is a *mutFile or *mutDir
type mutFileOrDir interface {
	Node
	invalidate()
	permanodeString() string
	eligibleToDelete() bool
}

func (n *mutFile) permanodeString() string {
	return n.blobref.String()
}

func (n *mutDir) permanodeString() string {
	return n.blobref.String()
}

func (n *mutFile) invalidate() {
	n.mu.Lock()
	n.deleted = true
	n.mu.Unlock()
}

func (n *mutDir) invalidate() {
	n.mu.Lock()
	n.deleted = true
	n.mu.Unlock()
}

func (n *mutFile) eligibleToDelete() bool {
	return n.localCreateTime.Before(time.Now().Add(-deletionRefreshWindow))
}

func (n *mutDir) eligibleToDelete() bool {
	return n.localCreateTime.Before(time.Now().Add(-deletionRefreshWindow))
}
