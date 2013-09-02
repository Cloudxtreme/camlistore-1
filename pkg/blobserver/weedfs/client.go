/*
Copyright 2013 Camlistore Authors.

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

package weedfs

import (
	"bytes"
	"encoding/base64"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	//"camlistore.org/third_party/github.com/cznic/kv"
	"camlistore.org/pkg/index/kvfile"
	"camlistore.org/pkg/sorted"
	"camlistore.org/third_party/github.com/tgulacsi/weedfs-client"
)

type client struct {
	db     sorted.KeyValue
	weedfs *weedfs.Client
}

// newClient creates a new client for the Weed-FS' masterURL
// using the given dbDir for the local DB, with the default kvfile storage.
func newClient(masterURL string, dbDir string) (*client, error) {
	name := filepath.Join(dbDir,
		"camli-"+base64.URLEncoding.EncodeToString([]byte(masterURL))+".kv")
	idx, err := kvfile.NewStorage(name)
	if err != nil {
		return nil, fmt.Errorf("couldn't open db as %s: %s", name, err)
	}
	return newIndexedClient(masterURL, idx)
}

// newIndexedClient creates a new client for the Weed-FS' masterURL
// using the given index.Storage
func newIndexedClient(masterURL string, idx sorted.KeyValue) (*client, error) {
	return &client{weedfs: weedfs.NewClient(masterURL), db: idx}, nil
}

// Close closes the underlying db
func (c *client) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

// Get returns the file data as an io.ReadCloser, and the size
func (c *client) Get(key string) (file io.ReadCloser, size int64, err error) {
	fileID, s, e := c.dbGet(key)
	if e != nil {
		err = e
		return
	}
	size = s
	file, err = c.weedfs.Download(fileID)
	return
}

func (c *client) dbGet(key string) (fileID string, size int64, err error) {
	val, e := c.db.Get(key)
	if e != nil {
		if e != os.ErrNotExist {
			err = fmt.Errorf("error getting key %q from db: %s", key, e)
		} else {
			err = e
		}
		return
	}
	err = decodeVal(val, &fileID, &size)
	return
}

// Put stores the file data
func (c *client) Put(key string, size int64, file io.Reader) error {
	var err error
	var obj Object
	if obj.FileID, err = c.weedfs.Upload(key, "application/octet-stream", file); err != nil {
		return err
	}
	obj.Size = size
	val, e := encodeVal(nil, obj.FileID, size)
	if e != nil {
		return e
	}
	if err = c.db.Set(key, string(val)); err != nil {
		err = fmt.Errorf("error setting key %q to %q: %s", key, obj, err)
	}
	return err
}

// Delete deletes the key from the backing Weed-FS and from the local db
func (c *client) Delete(key string) (err error) {
	fileID, _, err := c.dbGet(key)
	if err != nil {
		return err
	}
	mut := c.db.BeginBatch()
	mut.Delete(key)
	if err = c.weedfs.Delete(fileID); err != nil {
		return err
	}
	return c.db.CommitBatch(mut)
}

// Stat returns the size of the key's file
func (c *client) Stat(key string) (int64, error) {
	_, size, err := c.dbGet(key)
	return size, err
}

// Check checks the master's availability
func (c *client) Check() error {
	_, _, err := c.weedfs.Status()
	return err
}

// Object holds the info about an object: the keys and size
type Object struct {
	Key    string // Camlistore's key: the blobref
	FileID string // Weed-FS' key: the fileID
	Size   int64  // Size is the object's size
}

// List lists all the keys after the given key
func (c *client) List(after string, limit int) ([]Object, error) {
	enum := c.db.Find(after)
	n := limit / 2
	if limit < 1000 {
		n = limit
	}

	objs := make([]Object, 0, n)
	var (
		val string
		obj Object
		err error
	)
	for i := 0; i < limit && enum.Next(); i++ {
		obj.Key = enum.Key()
		if err = decodeVal(val, &obj.FileID, &obj.Size); err != nil {
			break
		}
		objs = append(objs, obj)
	}
	return objs, enum.Close()
}

func encodeVal(dst []byte, fileID string, size int64) ([]byte, error) {
	if dst == nil {
		dst = make([]byte, 0, 48)
	}
	buf := bytes.NewBuffer(dst)
	enc := gob.NewEncoder(buf)
	err := enc.Encode(size)
	if err != nil {
		return nil, err
	}
	err = enc.Encode(fileID)
	return buf.Bytes(), err
}
func decodeVal(val string, fileID *string, size *int64) error {
	dec := gob.NewDecoder(strings.NewReader(val))
	err := dec.Decode(size)
	if err != nil {
		return err
	}
	return dec.Decode(fileID)
}
