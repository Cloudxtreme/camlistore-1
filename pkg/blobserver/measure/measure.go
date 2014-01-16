/*
Copyright 2014 The Camlistore Authors

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

/*
Package measure creates statistics (count, sizes, compressability) from blobs.

Example low-level config:

     "/stats/": {
         "handler": "storage-measure",
         "handlerArgs": {
            "file": "/var/camlistore/blobs/stats.txt"
          }
     },


The measuring method is the following:
The histogram buckets are 2-base logarithmic: 2, 4, ... 128kB ... constants.MaxBlobSize=16Mb
We record average compression ratio for each bucket, for zlib and snappy compressions, too.
Just as the average duration of compression, and decompression, too.
The stats.txt will contain the following data:

*/
package measure

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/big"
	"os"
	"sync"
	"time"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/blobserver"
	"camlistore.org/pkg/constants"
	"camlistore.org/pkg/context"
	"camlistore.org/pkg/jsonconfig"

	"camlistore.org/third_party/code.google.com/p/snappy-go/snappy"
)

const zlibLevel = zlib.DefaultCompression
const bucketCount = 24 // 16Mb
const flushIntervalSeconds = 5

type statistics struct {
	file *os.File
	hist histogram
	sync.Mutex
}

func newFromConfig(_ blobserver.Loader, config jsonconfig.Obj) (storage blobserver.Storage, err error) {
	filename := config.RequiredString("file")
	if err := config.Validate(); err != nil {
		return nil, err
	}
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}
	s := &statistics{file: file}
	for i := uint(1); i < bucketCount; i++ {
		s.hist[int(i)].width = 1 << i
	}
	go func() {
		for _ = range time.Tick(time.Duration(flushIntervalSeconds) * time.Second) {
			_, err := s.file.Seek(0, 0)
			if err != nil {
				log.Printf("error seeking to the beginning of %s: %v", filename, err)
			}
			if err = s.file.Truncate(0); err != nil {
				log.Printf("error truncating %s: %v", filename, err)
				continue
			}
			func() {
				s.Lock()
				defer s.Unlock()
				if err = s.hist.WriteTo(s.file); err != nil {
					log.Printf("error writing measurement to %s: %v", filename, err)
				}
			}()
			if err = s.file.Sync(); err != nil {
				log.Printf("error syncing %s: %v", filename, err)
			}
		}
	}()
	return s, nil
}

func (s *statistics) Close() error { return s.file.Close() }

// ReceiveBlob measures some aspects of the given blob.
func (s *statistics) ReceiveBlob(br blob.Ref, source io.Reader) (sbr blob.SizedRef, err error) {
	log.Printf("measure got %v", br)
	raw, err := ioutil.ReadAll(&io.LimitedReader{source, constants.MaxBlobSize + 1})
	if len(raw) > constants.MaxBlobSize {
		err = fmt.Errorf("blob %v too big", br)
		return
	}
	rawSize := len(raw)
	sbr.Ref = br
	sbr.Size = int64(rawSize)
	n := big.NewInt(sbr.Size).BitLen()
	s.Lock()
	defer s.Unlock()
	s.hist[n].count++
	s.hist[n].rawBytes += uint64(rawSize)

	zlibSize, zlibCtime, zlibDtime := zlibBytes(raw)
	s.hist[n].zlibBytes += uint64(zlibSize)
	s.hist[n].zlibCtime += zlibCtime
	s.hist[n].zlibDtime += zlibDtime

	snappySize, snappyCtime, snappyDtime := snappyBytes(raw)
	s.hist[n].snappyBytes += uint64(snappySize)
	s.hist[n].snappyCtime += snappyCtime
	s.hist[n].snappyDtime += snappyDtime
	return
}

type histogram [bucketCount]bucket

func (h histogram) WriteTo(w io.Writer) error {
	var err error
	for i := 0; i < bucketCount; i++ {
		if h[i].rawBytes == 0 {
			continue
		}
		if _, err = fmt.Fprintf(w, "%d: %s\n", i, h[i]); err != nil {
			return err
		}
	}
	return nil
}

type bucket struct {
	width                    uint
	count                    int64
	rawBytes                 uint64
	zlibBytes                uint64
	zlibCtime, zlibDtime     time.Duration
	snappyBytes              uint64
	snappyCtime, snappyDtime time.Duration
}

func (b bucket) String() string {
	var zlibRatio, snappyRatio float64 = 1, 1
	var zlibCms, zlibDms, snappyCms, snappyDms uint64
	if b.rawBytes > 0 {
		zlibRatio = float64(b.zlibBytes) / float64(b.rawBytes)
		snappyRatio = float64(b.snappyBytes) / float64(b.rawBytes)
		zlibCms = uint64(b.zlibCtime) / b.rawBytes
		zlibDms = uint64(b.zlibDtime) / b.rawBytes
		snappyCms = uint64(b.snappyCtime) / b.rawBytes
		snappyDms = uint64(b.snappyDtime) / b.rawBytes
	}
	return fmt.Sprintf("Size %d - %d: %d blobs, %d bytes, zlib: ratio=%.03f encode=%d ms/b decode=%d ms/b, snappy: ratio=%.03f encode=%d ms/b decode=%d ms/b",
		1<<b.width, (1<<(b.width+1))-1, b.count, b.rawBytes,
		zlibRatio, zlibCms, zlibDms,
		snappyRatio, snappyCms, snappyDms)
}

func zlibBytes(raw []byte) (clen int, ctime, dtime time.Duration) {
	var b bytes.Buffer
	t := time.Now()
	z, _ := zlib.NewWriterLevel(&b, zlibLevel)
	z.Write(raw)
	z.Close()
	ctime = time.Since(t)
	clen = b.Len()

	zr, _ := zlib.NewReader(bytes.NewReader(b.Bytes()))
	t = time.Now()
	ioutil.ReadAll(zr)
	dtime = time.Since(t)
	return
}

func snappyBytes(raw []byte) (clen int, ctime, dtime time.Duration) {
	t := time.Now()
	z, _ := snappy.Encode(nil, raw)
	ctime = time.Since(t)
	clen = len(z)

	t = time.Now()
	snappy.Decode(nil, z)
	dtime = time.Since(t)
	return
}

func timeIt(fun func([]byte) int, b []byte) (int, time.Duration) {
	t := time.Now()
	res := fun(b)
	return res, time.Since(t)
}

func init() {
	blobserver.RegisterStorageConstructor("measure", blobserver.StorageConstructor(newFromConfig))
}

func (s *statistics) FetchStreaming(br blob.Ref) (io.ReadCloser, int64, error) {
	return nil, 0, os.ErrNotExist
}
func (s *statistics) RemoveBlobs(blobs []blob.Ref) error {
	return blobserver.ErrNotImplemented
}
func (s *statistics) StatBlobs(dest chan<- blob.SizedRef, blobs []blob.Ref) error {
	return os.ErrNotExist
}
func (s *statistics) EnumerateBlobs(ctx *context.Context, dest chan<- blob.SizedRef, after string, limit int) error {
	close(dest)
	return nil
}
