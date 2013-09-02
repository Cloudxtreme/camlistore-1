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
	"camlistore.org/pkg/syncutil"
)

// concurrent connection number
const DefaultConcurrency = 8

// parallel runs the todo function for every blobref coming in the blobs array,
// (using concurrency number of goroutines) and returns the cumulated error
func parallel(blobs []blob.Ref, concurrency int, todo func(blob.Ref) error) error {
    n := len(blobs)
	switch n{
        case 0: return nil
    case 1: return todo(blobs[0])
	}

	if concurrency > n {
		concurrency = n
	} else if concurrency < 1 {
		concurrency = DefaultConcurrency
	}

	statGate := syncutil.NewGate(concurrency)
	var wg syncutil.Group

	for _, br := range blobs {
		br := br
		statGate.Start()
		wg.Go(func() error {
			defer statGate.Done()

			return todo(br)
		})
	}
	return wg.Err()
}
