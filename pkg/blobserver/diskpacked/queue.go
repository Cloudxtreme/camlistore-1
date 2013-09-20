/*
Copyright 2013 Google Inc.

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
	"path/filepath"

	"camlistore.org/pkg/blobserver"
	"camlistore.org/pkg/blobserver/localdisk"
)

// CreateQueue creates a new queue in which all new uploads go to
// both the root storage as well as the named queue, which is then returned.
func (s *storage) CreateQueue(name string) (blobserver.Storage, error) {
	q, err := localdisk.New(filepath.Join(s.root, name))
	if err != nil {
		return nil, err
	}
	s.mirrorPartitions = append(s.mirrorPartitions, q)
	return q, nil
}
