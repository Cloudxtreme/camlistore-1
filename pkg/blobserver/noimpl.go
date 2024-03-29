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

package blobserver

import (
	"errors"
	"io"
	"os"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/types"
)

// NoImplStorage is an implementation of Storage that returns a not
// implemented error for all operations.
type NoImplStorage struct{}

var _ Storage = (*NoImplStorage)(nil)

func (nis *NoImplStorage) Fetch(blob.Ref) (file types.ReadSeekCloser, size int64, err error) {
	return nil, 0, os.ErrNotExist
}

func (nis *NoImplStorage) FetchStreaming(blob.Ref) (file io.ReadCloser, size int64, err error) {
	return nil, 0, os.ErrNotExist
}

func (nis *NoImplStorage) ReceiveBlob(blob.Ref, io.Reader) (sb blob.SizedRef, err error) {
	err = errors.New("ReceiveBlob not implemented")
	return
}

func (nis *NoImplStorage) StatBlobs(dest chan<- blob.SizedRef, blobs []blob.Ref) error {
	return errors.New("Stat not implemented")
}

func (nis *NoImplStorage) EnumerateBlobs(dest chan<- blob.SizedRef, after string, limit int) error {
	return errors.New("EnumerateBlobs not implemented")
}

func (nis *NoImplStorage) RemoveBlobs(blobs []blob.Ref) error {
	return errors.New("Remove not implemented")
}
