package tef

import (
	"bytes"
	"os"
	"testing"
)

type Closer struct {
	bytes.Buffer
}

func (*Closer) Close() error { return nil }

var currentTime uint64 = 0

func timestampTest() uint64 {
	currentTime += 100
	return currentTime
}
