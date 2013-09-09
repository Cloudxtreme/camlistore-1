// Package bootchart creates the directory of files that can be consumed by
// pybootchartgui.py ( https://github.com/mmeeks/bootchart ) and possibly
// bootchart (http://www.bootchart.org/) (untested).
package bootchart

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"
)

func timestampNow() uint64 {
	return uint64(time.Now().Nanosecond()) * 10000
}

// Override for testing
var timestamp = timestampNow

// A Writer provides writers to generate a directory of log files compatible
// with the bootchart visualizer.
type Writer struct {
	dsw, hw, tsw, sw io.WriteCloser
	// TODO(wathiede): add Hz to dictate polling frequency.
}

type Header struct {
	version, title, uname, release, cpu, cmdline string
}

// These are named after the C struct taskstats
// https://www.kernel.org/doc/Documentation/accounting/taskstats-struct.txt
type Taskstats struct {
	Pid              uint32
	Ppid             uint32
	Command          string
	CpuRunRealTotal  uint64
	BlkioDelayTotal  uint64
	SwapinDelayTotal uint64
}

// NewWriter returnes a new Writer, creates and opens files under path. If
// there is any failure creating a file, err will be the first error
// encountered and all open files will be closed.
// TODO(wathiede): setup callbacks, or take an interface that implements each
// stat type and can be called in a timed loop to periodically report stats.
func NewWriter(path string) (w *Writer, err error) {
	defer func() {
		if err != nil {
			if w.dsw != nil {
				w.dsw.Close()
			}
			if w.hw != nil {
				w.hw.Close()
			}
			if w.tsw != nil {
				w.tsw.Close()
			}
			if w.sw != nil {
				w.sw.Close()
			}
		}
	}()
	w = new(Writer)
	w.hw, err = os.Create(filepath.Join(path, "header"))
	if err != nil {
		return
	}
	w.dsw, err = os.Create(filepath.Join(path, "proc_diskstats.log"))
	if err != nil {
		return
	}
	w.tsw, err = os.Create(filepath.Join(path, "taskstats.log"))
	if err != nil {
		return
	}
	w.sw, err = os.Create(filepath.Join(path, "proc_stat.log"))
	if err != nil {
		return
	}
	return
}

// Close attempts to close all io.WriteClosers in Writer.  The last error
// encountered is returned.
func (w *Writer) Close() (err error) {
	e := w.dsw.Close()
	if e != nil {
		err = e
	}
	e = w.hw.Close()
	if e != nil {
		err = e
	}
	e = w.tsw.Close()
	if e != nil {
		err = e
	}
	e = w.sw.Close()
	if e != nil {
		err = e
	}
	return
}

// WriteDiskStats outputs the contents of 'proc_diskstats.log' to w, used for
// filling out the Disk throughput/utilization stacked graph at the top of the
// bootchart.
func (w *Writer) WriteDiskStats() error {
	// TODO(wathiede): minimal file required to make bootchart happy.
	//
	// The file format is two or more samples in the pattern:
	//  <timestamp in seconds * 100>
	//  <contents of /proc/diskstats>
	//  <newline>
	_, err := fmt.Fprint(w.dsw, `0
   1       0 ram0 0 0 0 0 0 0 0 0 0 0 0

1
   1       0 ram0 0 0 0 0 0 0 0 0 0 0 0
   `)
	return err
}

// WriteHeader outputs the contents of 'header' to w, used for filling out
// the bootchart title.
func (w *Writer) WriteHeader(h Header) error {
	_, err := fmt.Fprintf(w.hw, `version = %s
title = %s
system.uname = %s
system.release = %s
system.cpu = %s
system.kernel.options = %s
`, h.version, h.title, h.uname, h.release, h.cpu, h.cmdline)
	return err
}

// WriteTaskstats outputs the contents of 'taskstats.log' to w, used for
// filling out the Gantt section of bootchart.
//
// Notes:
//   Task stats may be omitted if they haven't changed between samples.  If a
//   process is absent from a round of sampling it is assumed to be idle.  The
//   next time a sample is present, one of cpu_run_real_total,
//   blkio_delay_total, or swapin_delay_total must have incremented, and is
//   then considered 'high' for the intervals in a row it is present and
//   increasing.
//
//   cpu_run_real_total is used for coloring blue into the line, aka 'Running
//   	(%cpu)'.
//	 blkio_delay_total and swapin_delay_total are used for coloring red into
//	 	the line, aka 'Unint.sleep (I/O)'.
func (w *Writer) WriteTaskstats(stats []Taskstats) error {
	// The file format is two or more samples in the pattern:
	//   <timestamp in seconds * 100>
	//   <zero or more lines containing per-process stats>
	//   <newline>
	//
	// The per-process stats are:
	//   pid, ppid, comm, cpu_run_real_total, blkio_delay_total,
	//   swapin_delay_total
	_, err := fmt.Fprintf(w.tsw, "%d\n", timestamp())
	for _, s := range stats {
		_, err = fmt.Fprintf(w.tsw, "%d %d (%s) %d %d %d\n", s.Pid, s.Ppid,
			s.Command, s.CpuRunRealTotal, s.BlkioDelayTotal,
			s.SwapinDelayTotal)
		if err != nil {
			return err
		}
	}
	_, err = fmt.Fprintln(w.tsw)
	return nil
}

// WriteStat outputs the contents of 'proc_stat.log' to w, used for
// filling out CPU / I/O stacked graph at the top of the bootchart.
func (w *Writer) WriteStat() error {
	// TODO(wathiede): minimal file required to make bootchart happy.
	//
	// The file format is two or more samples in the pattern:
	//  <timestamp in seconds * 100>
	//  <contents of /proc/stat>
	//  <newline>
	_, err := fmt.Fprint(w.sw, `0
cpu  0 0 0 0 0 0 0 0 0
cpu0 0 0 0 0 0 0 0 0 0

1
cpu  0 0 0 0 0 0 0 0 0
cpu0 0 0 0 0 0 0 0 0 0
`)
	return err
}
