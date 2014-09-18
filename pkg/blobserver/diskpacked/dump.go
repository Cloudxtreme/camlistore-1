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

package diskpacked

import (
	"archive/tar"
	"io"
	"log"
	"os"

	"camlistore.org/pkg/blob"
	"camlistore.org/pkg/context"
)

// Dump dumps the contents of the diskpacked .pack files to the specified writer,
// in tar format.
func Dump(dst io.Writer, root string) error {
	// there is newStorage, but that may open a file for writing
	var s = &storage{root: root}

	w := tar.NewWriter(dst)

	ctx := context.TODO() // TODO(tgulacsi): get the verbosity from context
	for i := 0; ; i++ {
		fh, err := os.Open(s.filename(i))
		if err != nil {
			if os.IsNotExist(err) {
				break
			}
			return err
		}
		err = s.dumpOne(ctx, w, fh, i)
		fh.Close()
		if err != nil {
			return err
		}
	}
	return w.Close()
}

type infoReaderAt interface {
	io.ReaderAt
	Stat() (os.FileInfo, error)
}

func (s *storage) dumpOne(ctx *context.Context, tw *tar.Writer, src infoReaderAt, packID int) error {
	// TODO(tgulacsi): proper verbose from context
	verbose := camliDebug
	fi, err := src.Stat()
	if err != nil {
		return err
	}
	hdr, err := tar.FileInfoHeader(fi, "")
	if err != nil {
		return err
	}
	err = s.walkPack(verbose, packID,
		func(packID int, ref blob.Ref, offset int64, size uint32) error {
			if !ref.Valid() {
				if camliDebug {
					log.Printf("found deleted blob in %d at %d with size %d", packID, offset, size)
				}
				return nil
			}
			hdr.Name, hdr.Size = ref.String(), int64(size)
			if err := tw.WriteHeader(hdr); err != nil {
				return err
			}
			_, err := io.Copy(tw, io.NewSectionReader(src, offset, int64(size)))
			return err
		})
	if err != nil {
		return err
	}

	return tw.Close()
}
