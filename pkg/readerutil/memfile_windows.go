// +build windows

package readerutil

import "os"

// removeEarly is a no-op on Windows
func removeEarly(fn string) {
	return
}

// removeAtlast removes the file
func removeAtlast(fn string) error {
	return os.Remove(fn)
}
