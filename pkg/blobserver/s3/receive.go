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

package s3

import (
	"crypto/md5"
	"hash"
	"io"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/readerutil"
	"camlistore.org/pkg/types"
)

// amazonSlurper slurps up a blob to memory (or spilling to disk if
// over maxInMemorySlurp) to verify its digest (and also gets its MD5
// for Amazon's Content-MD5 header, even if the original blobref
// is e.g. sha1-xxxx)
type amazonSlurper struct {
	types.ReadWriteSeekCloser
	md5 hash.Hash
}

func newAmazonSlurper() *amazonSlurper {
	return &amazonSlurper{
		ReadWriteSeekCloser: readerutil.NewMemorySlurper(),
		md5:                 md5.New(),
	}
}

func (as *amazonSlurper) Write(p []byte) (n int, err error) {
	as.md5.Write(p)
	return as.ReadWriteSeekCloser.Write(p)
}

func (sto *s3Storage) ReceiveBlob(b blob.Ref, source io.Reader) (sr blob.SizedRef, err error) {
	slurper := newAmazonSlurper()
	defer slurper.Close()

	size, err := io.Copy(slurper, source)
	if err != nil {
		return sr, err
	}
	err = sto.s3Client.PutObject(b.String(), sto.bucket, slurper.md5, size, slurper)
	if err != nil {
		return sr, err
	}
	return blob.SizedRef{Ref: b, Size: size}, nil
}
