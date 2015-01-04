/*
Copyright 2014 The Camlistore Authors.

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

// Package bolt provides an implementation of sorted.KeyValue
// on top of a single mutable database file on disk using
// github.com/syndtr/gobolt.
package boltdb

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"camlistore.org/pkg/jsonconfig"
	"camlistore.org/pkg/sorted"

	"camlistore.org/third_party/github.com/boltdb/bolt"
)

var _ sorted.Wiper = (*kvis)(nil)

func init() {
	sorted.RegisterKeyValue("boltdb", newKeyValueFromJSONConfig)
}

// NewStorage is a convenience that calls newKeyValueFromJSONConfig
// with file as the bolt storage file.
func NewStorage(file string) (sorted.KeyValue, error) {
	return newKeyValueFromJSONConfig(jsonconfig.Obj{"file": file})
}

// newKeyValueFromJSONConfig returns a KeyValue implementation on top of a
// github.com/boltdb/bolt file.
func newKeyValueFromJSONConfig(cfg jsonconfig.Obj) (sorted.KeyValue, error) {
	file := cfg.RequiredString("file")
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	opts := &bolt.Options{Timeout: time.Second}
	// Timeout is the time bolt waits for locking the file.
	db, err := bolt.Open(file, 0640, opts)
	if err != nil {
		return nil, err
	}
	if os.Getenv("CAMLI_DEV_CAMLI_ROOT") != "" {
		// Be more strict in dev mode.
		db.StrictMode = true
	}
	if err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	}); err != nil {
		db.Close()
		return nil, err
	}
	is := &kvis{
		db:   db,
		path: file,
		opts: opts,
	}
	return is, nil
}

type kvis struct {
	path string
	db   *bolt.DB
	opts *bolt.Options
	mu   sync.RWMutex   // protecting db on change
	wg   sync.WaitGroup // for waiting opened transactions before close
}

func (is *kvis) Get(key string) (string, error) {
	is.mu.RLock()
	defer is.mu.RUnlock()

	var val string
	err := is.db.View(func(tx *bolt.Tx) error {
		v := mustGetBucket(tx).Get([]byte(key))
		if v == nil {
			return sorted.ErrNotFound
		}
		val = string(v)
		return nil
	})
	return val, err
}

func (is *kvis) Set(key, value string) error {
	is.mu.RLock()
	defer is.mu.RUnlock()

	return is.db.Update(func(tx *bolt.Tx) error {
		return mustGetBucket(tx).Put([]byte(key), []byte(value))
	})
}

func (is *kvis) Delete(key string) error {
	is.mu.RLock()
	defer is.mu.RUnlock()

	return is.db.Update(func(tx *bolt.Tx) error {
		return mustGetBucket(tx).Delete([]byte(key))
	})
}

func (is *kvis) Close() error {
	is.mu.Lock()
	defer is.mu.Unlock()

	log.Printf("Wait for all transactions to finish...")
	is.wg.Wait()
	log.Printf("Transactions has finished.")

	db := is.db
	is.db = nil
	return db.Close()
}

func (is *kvis) Find(start, end string) sorted.Iterator {
	is.mu.RLock()
	defer is.mu.RUnlock()

	tx, err := is.db.Begin(false)
	if err != nil {
		is.mu.RUnlock()
		return &iter{err: err}
	}
	is.wg.Add(1)
	it := &iter{cu: mustGetBucket(tx).Cursor(), onClose: is.wg.Done}

	if start != "" {
		it.init = func() ([]byte, []byte) { return it.cu.Seek([]byte(start)) }
	} else {
		it.init = it.cu.First
	}
	if end != "" {
		it.end = []byte(end)
	}
	return it
}

func (is *kvis) BeginBatch() sorted.BatchMutation {
	is.mu.RLock()
	defer is.mu.RUnlock()

	is.wg.Add(1)
	bb := &bbatch{db: is.db, onClose: is.wg.Done}
	bb.mu.Lock()
	defer bb.mu.Unlock()

	_ = bb.flush(true)
	return bb
}

func (is *kvis) Wipe() error {
	log.Printf("Wipe %q", is.path)
	is.mu.Lock()
	defer is.mu.Unlock()

	log.Printf("Waiting for pending transactions...")
	is.wg.Wait()
	log.Printf("Transactions has finished.")

	db := is.db
	is.db = nil
	// Close the already open DB.
	if err := db.Close(); err != nil {
		return err
	}
	if err := os.Remove(is.path); err != nil {
		return err
	}

	db, err := bolt.Open(is.path, 0640, is.opts)
	if err != nil {
		return fmt.Errorf("error creating %s: %v", is.path, err)
	}
	is.db = db
	return nil
}

// Limit transaction sizes
//
// Quote from camlistore.org/third_party/github.com/boltdb/bolt/README.md:
// Bulk loading a lot of random writes into a new bucket can be slow as
// the page will not split until the transaction is committed.
// Randomly inserting more than 100,000 key/value pairs into a single
// new bucket in a single transaction is not advised.
const bbTranLimit = 1000

type bbatch struct {
	onClose func()
	mu      sync.Mutex
	db      *bolt.DB
	bu      *bolt.Bucket
	n       int
	err     error
}

// flush the transaction, open a new if createNew is true.
// This function asserts that bbatch.mu is already locked!
func (bb *bbatch) flush(createNew bool) error {
	if bb.err != nil {
		return bb.err
	}
	if bb.bu != nil {
		tx := bb.bu.Tx()
		bb.bu = nil
		if bb.err = tx.Commit(); bb.err != nil {
			log.Printf("flush commit error: %v", bb.err)
			return bb.err
		}
	}
	if !createNew {
		return nil
	}
	tx, err := bb.db.Begin(true)
	if err != nil {
		log.Printf("flush new transaction: %v", err)
		bb.err = err
		return err
	}
	bb.bu = mustGetBucket(tx)
	bb.n = 0
	return nil
}

func (bb *bbatch) Set(key, value string) {
	bb.mu.Lock()
	defer bb.mu.Unlock()

	if bb.err != nil {
		return
	}
	if bb.err = bb.bu.Put([]byte(key), []byte(value)); bb.err != nil {
		return
	}
	bb.n++
	if bb.n > bbTranLimit {
		_ = bb.flush(true)
	}
}

func (bb *bbatch) Delete(key string) {
	bb.mu.Lock()
	defer bb.mu.Unlock()

	if bb.err != nil {
		return
	}
	if bb.err = bb.bu.Delete([]byte(key)); bb.err != nil {
		return
	}
	bb.n++
	if bb.n > bbTranLimit {
		_ = bb.flush(true)
	}
}

func (is *kvis) CommitBatch(bm sorted.BatchMutation) error {
	bb, ok := bm.(*bbatch)
	if !ok {
		return errors.New("invalid batch type")
	}
	bb.mu.Lock()
	defer bb.mu.Unlock()
	defer func() {
		if bb.onClose != nil {
			bb.onClose()
			bb.onClose = nil
		}
		bb.db = nil
	}()

	if bb.err != nil {
		return bb.err
	}
	return bb.flush(false)
}

type iter struct {
	mu   sync.Mutex
	cu   *bolt.Cursor
	init func() ([]byte, []byte)
	end  []byte

	key, val   []byte
	skey, sval *string // for caching string values

	err     error
	closed  bool
	onClose func()
}

func (it *iter) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.onClose != nil {
		defer it.onClose()
	}
	cu := it.cu
	it.cu = nil
	err := cu.Bucket().Tx().Rollback()
	if it.err != nil {
		return it.err
	}
	return err
}

func (it *iter) KeyBytes() []byte {
	return it.key
}

func (it *iter) Key() string {
	if it.skey != nil {
		return *it.skey
	}
	str := string(it.key)
	it.skey = &str
	return str
}

func (it *iter) ValueBytes() []byte {
	return it.val
}

func (it *iter) Value() string {
	if it.sval != nil {
		return *it.sval
	}
	str := string(it.val)
	it.sval = &str
	return str
}

func (it *iter) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.err != nil {
		return false
	}
	it.skey, it.sval = nil, nil
	if it.init != nil {
		it.key, it.val = it.init()
		it.init = nil
	} else {
		it.key, it.val = it.cu.Next()
	}
	if it.key == nil || it.end != nil && bytes.Compare(it.key, it.end) >= 0 {
		// reached end
		return false
	}
	return true
}

var bucketName = []byte("/")

func mustGetBucket(tx *bolt.Tx) *bolt.Bucket {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("PANIC getting bucket %q of %v: %v", bucketName, tx, r)
			panic(r)
		}
	}()
	bucket := tx.Bucket(bucketName)
	if bucket == nil {
		panic(fmt.Sprintf("NIL bucket of %v", tx))
	}
	return bucket
}
