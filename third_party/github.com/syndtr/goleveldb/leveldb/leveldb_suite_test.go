package leveldb

import (
	"testing"

	. "camlistore.org/third_party/github.com/onsi/ginkgo"
	. "camlistore.org/third_party/github.com/onsi/gomega"

	"camlistore.org/third_party/github.com/syndtr/goleveldb/leveldb/testutil"
)

func TestLeveldb(t *testing.T) {
	testutil.RunDefer()

	RegisterFailHandler(Fail)
	RunSpecs(t, "Leveldb Suite")

	RegisterTestingT(t)
	testutil.RunDefer("teardown")
}
