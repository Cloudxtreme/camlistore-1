package tef

import (
	"log"
	"os"
	"strings"
	"sync"
)

// SetupCmdWriter sets up a stats writer which writes into the given filename.
func SetupCmdWriter(fn string) (ch chan<- []Taskstats, closer func() error, err error) {
	fh, err := os.OpenFile(fn, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0640)
	if err != nil {
		return nil, nil, err
	}
	tw, err := NewTaskWriter(fh, strings.HasSuffix(fn, ".gz"))
	if err != nil {
		_ = fh.Close()
		return nil, nil, err
	}

	tch := make(chan []Taskstats)
	ch = tch
	var wg sync.WaitGroup
	var closeErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		for stats := range tch {
			if err = tw.Add(stats...); err != nil {
				log.Printf("cannot add stats: %v", err)
				continue
			}
		}
		if err := tw.Close(); err != nil {
			closeErr = err
		}
		if err := fh.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}()

	closer = func() error {
		close(tch)
		wg.Wait()
		return closeErr
	}

	return ch, closer, nil
}
