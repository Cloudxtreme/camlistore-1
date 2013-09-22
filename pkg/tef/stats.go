// Package tef writes Trace Event Format, which has a nice viewer:
// http://google.github.io/trace-viewer/ .
package tef

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"time"
)

func timestampNow() uint64 {
	return uint64(time.Now().UnixNano()) / uint64(time.Microsecond)
}

// Override for testing
var timestamp = timestampNow

// These are named after the C struct taskstats
// https://www.kernel.org/doc/Documentation/accounting/taskstats-struct.txt
type Taskstats struct {
	Pid              uint32
	Ppid             uint32
	Command          string
	CPURunRealTotal  uint64
	BlkioDelayTotal  uint64
	SwapinDelayTotal uint64
}

type flusher interface {
	Flush() error
}
type writeFlushCloser interface {
	io.WriteCloser
	flusher
}
type tefWriter struct {
	w        writeFlushCloser
	notFirst bool
}

// TaskWriter is the interface for writing tasks.
//
// Task stats may be omitted if they haven't changed between samples.  If a
// process is absent from a round of sampling it is assumed to be idle.  The
// next time a sample is present, one of cpu_run_real_total,
// blkio_delay_total, or swapin_delay_total must have incremented, and is
// then considered 'high' for the intervals in a row it is present and
// increasing.
//
// cpu_run_real_total is used for coloring blue into the line, aka 'Running
// 	   (%cpu)'.
// blkio_delay_total and swapin_delay_total are used for coloring red into
//	 	the line, aka 'Unint.sleep (I/O)'.
type TaskWriter interface {
	io.Closer
	Add(tasks ...Taskstats) error
}

// NewTaskWriter returns a new Taskstats writer.
func NewTaskWriter(w io.Writer, compress bool) (TaskWriter, error) {
	var tw *tefWriter
	if compress {
		tw = &tefWriter{w: gzip.NewWriter(w)}
	} else {
		bw := bufio.NewWriter(w)
		tw = &tefWriter{w: struct {
			io.Writer
			flusher
			io.Closer
		}{bw, bw, ioutil.NopCloser(nil)}}
	}

	if _, err := tw.w.Write([]byte("[\n")); err != nil {
		return tw, err
	}
	return tw, nil
}

func (tw *tefWriter) Close() error {
	if _, err := tw.w.Write([]byte("\n]")); err != nil {
		return err
	}
	err := tw.w.Flush()
	if closeErr := tw.w.Close(); closeErr != nil && err == nil {
		return closeErr
	}
	return err
}

func (tw *tefWriter) Add(stats ...Taskstats) error {
	// name: The name of the event, as displayed in Trace Viewer
	// cat: The event categories. This is a comma separated list of categories for the event. The categories can be used to hide events in the Trace Viewer UI.
	// ph: The event type. This is a single character which changes depending on the type of event being output. The valid values are B, E, I, C, S, T, F, s, t, f, M, P, O, N, D. We will discuss each phase type below.
	// ts: The timestamp of the event. The timestamps are provided at microsecond granularity.
	// pid: The process ID for the process that output this event.
	// tid: The thread ID for the thread that output this event.
	// args: Any arguments provided for the event. Some of the event types have required argument fields, otherwise, you can put any information you wish in here. The arguments are displayed in Trace Viewer when you view an event in the analysis section.
	for _, s := range stats {
		if tw.notFirst {
			if _, err := tw.w.Write([]byte{','}); err != nil {
				return err
			}
		} else {
			tw.notFirst = true
		}
		if _, err := fmt.Fprintf(tw.w,
			`{"name":%q,"cat":"%s","ph":"%s","ts":%d,"pid":%d,"tid":%d,"args":{}}`+"\n",
			s.Command, "", "X", timestampNow(), s.Pid, 0,
		); err != nil {
			return err
		}
	}
	return nil
}
