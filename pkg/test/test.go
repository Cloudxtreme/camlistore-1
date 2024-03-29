/*
Copyright 2013 The Camlistore Authors

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

package test

import (
	"os"
	"strconv"
	"testing"
)

// BrokenTest marks the test as broken and calls t.Skip, unless the environment
// variable RUN_BROKEN_TESTS is set to 1 (or some other boolean true value).
func BrokenTest(t *testing.T) {
	if v, _ := strconv.ParseBool(os.Getenv("RUN_BROKEN_TESTS")); !v {
		t.Skipf("Skipping broken tests without RUN_BROKEN_TESTS=1")
	}
}
