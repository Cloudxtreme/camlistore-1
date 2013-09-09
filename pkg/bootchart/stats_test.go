package bootchart

import (
	"bytes"
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

func TestWriteBootchartDir(t *testing.T) {
	timestamp = timestampTest

	//	var dsw, hw, tsw, sw Closer
	//	w := &Writer{
	//		dsw: &dsw,
	//		hw:  &hw,
	//		tsw: &tsw,
	//		sw:  &sw,
	//	}
	w, err := NewWriter("/tmp/t/")
	if err != nil {
		t.Fatal(err)
	}

	err = w.WriteHeader(Header{
		version: "version string",
		title:   "title string",
		uname:   "uname string",
		release: "release string",
		cpu:     "cpu string",
		cmdline: "cmdline string",
	})

	if err != nil {
		t.Error("Failed to write header", err)
	}

	err = w.WriteDiskStats()
	if err != nil {
		t.Error("Failed to disk stats", err)
	}

	err = w.WriteStat()
	if err != nil {
		t.Error("Failed to stat", err)
	}

	want := `version = version string
title = title string
system.uname = uname string
system.release = release string
system.cpu = cpu string
system.kernel.options = cmdline string
`
	_ = want
	//got := hw.String()
	//if want != got {
	//	t.Errorf("Got %q want %q", got, want)
	//}

	// pybootchart breaks if the total sum of cpu or IO time equals zero.  So
	// we set a non-zero number on the grandparent process.
	stats := []Taskstats{
		Taskstats{CpuRunRealTotal: 1, BlkioDelayTotal: 1, Pid: 100, Ppid: 1, Command: "grandparent"},
		Taskstats{Pid: 200, Ppid: 100, Command: "parent-1"},
		Taskstats{Pid: 201, Ppid: 200, Command: "sub-1-1"},
		Taskstats{Pid: 202, Ppid: 200, Command: "sub-1-2"},
		Taskstats{Pid: 300, Ppid: 100, Command: "parent-2"},
		Taskstats{Pid: 301, Ppid: 300, Command: "sub-2-1"},
		Taskstats{Pid: 302, Ppid: 300, Command: "sub-2-2"},
	}

	y, n := true, false
	// Determine what tasks should be considered running at each timestep.
	running := [][]bool{
		[]bool{n, n, n, n, n, n, n},
		[]bool{y, n, n, n, n, n, n},
		// parent-1 starts
		[]bool{y, y, n, n, n, n, n},
		[]bool{y, y, y, n, n, n, n},
		[]bool{y, y, y, y, n, n, n},
		[]bool{y, y, n, y, n, n, n},
		[]bool{y, y, n, n, n, n, n},
		// parent-1 ends
		// parent-2 starts
		[]bool{y, n, n, n, n, n, n},
		[]bool{y, n, n, n, y, n, n},
		[]bool{y, n, n, n, y, y, n},
		[]bool{y, n, n, n, y, y, y},
		[]bool{y, n, n, n, y, n, y},
		[]bool{y, n, n, n, y, n, n},
		// parent-2 ends
	}

	for _, r := range running {
		cur := []Taskstats{}
		for i, v := range r {
			if v {
				stats[i].CpuRunRealTotal += 1
				cur = append(cur, stats[i])
			}
		}

		err = w.WriteTaskstats(cur)
		if err != nil {
			t.Error("Failed to write taskstats", err)
		}
	}

	err = w.Close()
	if err != nil {
		t.Error(err)
	}
}
