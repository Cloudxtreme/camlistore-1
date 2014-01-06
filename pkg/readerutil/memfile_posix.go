// +build !windows

package readerutil

import "os"

// removeEarly removes the filename
func removeEarly(fn string) {
	os.Remove(fn)
}

// removeAtlast removes the file if exists
func removeAtlast(fn string) error {
	if err := os.Remove(fn); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
