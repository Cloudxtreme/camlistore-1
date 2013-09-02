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
	"errors"
	"runtime"
	"sync"
	"testing"

	"camlistore.org/pkg/blob"
)

func TestParallel1(t *testing.T) {
	runtime.GOMAXPROCS(1) // ensure a known value (2 workers)

	var err error
	for _, i := range []int{2, 3, 5, 7, 13, 991, 1} {
		t.Logf("starting %d mkFailAfter(%d)", i, i+1)
		err = parallel(make([]blob.Ref, i), mkFailAfter(int32(i)+1))
		if err != nil {
			t.Errorf("failed what can't fail(i=%d): %s (%v %T)", i, err, err, err)
			t.FailNow()
		}
		t.Logf("starting %d mkFailAfter(0)", i)
		if err = parallel(make([]blob.Ref, i), mkFailAfter(0)); err == nil {
			t.Errorf("didn't fail what must fail(i=%d): %s", i, err)
		}
	}
}

var errAfter = errors.New("failAfter limit reached")

func mkFailAfter(failAfter int32) func(blob.Ref) error {
	mtx := new(sync.Mutex)
	n := &failAfter
	return func(br blob.Ref) error {
		mtx.Lock()
		defer mtx.Unlock()
		if *n <= 0 {
			return errAfter
		}
		*n--
		return nil
	}
}
