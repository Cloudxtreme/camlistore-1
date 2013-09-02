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

/*
Package weedfs registers the "weedfs" blobserver storage type, storing
blobs in Weed-FS.

Example low-level config:

     "/r1/": {
         "handler": "storage-weedfs",
         "handlerArgs": {
            "master": "http://localhost:9333",
          }
     },

*/
package weedfs

import (
	"fmt"

	"camlistore.org/pkg/blobserver"
	"camlistore.org/pkg/jsonconfig"
)

type weedfsStorage struct {
	//*blobserver.SimpleBlobHubPartitionMap
	weedfsClient *client
}

func newFromConfig(_ blobserver.Loader, config jsonconfig.Obj) (storage blobserver.Storage, err error) {
	masterURL, dbDir := config.RequiredString("masterURL"), config.RequiredString("dbDir")
	skipStartupCheck := config.OptionalBool("skipStartupCheck", false)
	if err := config.Validate(); err != nil {
		return nil, err
	}
	stor := new(weedfsStorage)
	if stor.weedfsClient, err = newClient(masterURL, dbDir); err != nil {
		return nil, fmt.Errorf("Cannot create client: %s", err)
	}
	if !skipStartupCheck { // not expensive, but fails if Weed-FS master is unaccessible
		if err = stor.weedfsClient.Check(); err != nil {
			return nil, fmt.Errorf("WeedFS master check error: %s", err)
		}
	}
	return stor, nil
}

func init() {
	blobserver.RegisterStorageConstructor("weedfs", blobserver.StorageConstructor(newFromConfig))
}
