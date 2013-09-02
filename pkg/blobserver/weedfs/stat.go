/*
Copyright 2013 Camlistore Authors

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
	"camlistore.org/pkg/blob"
)

// Stat checks for the existence of blobs, writing their sizes
// (if found back to the dest channel), and returning an error
// or nil.  Stat() should NOT close the channel.
// wait is the max time to wait for the blobs to exist,
// or 0 for no delay.
func (sto *weedfsStorage) StatBlobs(dest chan<- blob.SizedRef, blobs []blob.Ref) error {
	return parallel(blobs, 0, func(br blob.Ref) error {
		size, err := sto.weedfsClient.Stat(br.String())
		if err == nil {
			dest <- blob.SizedRef{Ref: br, Size: size}
		}
		return err
	})
}
