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

package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"

	"camlistore.org/pkg/blobserver/diskpacked"
	"camlistore.org/pkg/cmdmain"
	"camlistore.org/pkg/jsonconfig"
	"camlistore.org/pkg/osutil"
	"camlistore.org/pkg/serverinit"
)

type dumpDpCmd struct {
	output string
}

func init() {
	cmdmain.RegisterCommand("dump-diskpacked",
		func(flags *flag.FlagSet) cmdmain.CommandRunner {
			cmd := &dumpDpCmd{}
			flags.StringVar(&cmd.output, "o", "-",
				"Tarfile name to output to. If empty, stdout will be used.")
			return cmd
		})
}

func (c *dumpDpCmd) Describe() string {
	return "Dump the contents of the diskpacked blob store"
}

func (c *dumpDpCmd) Usage() {
	fmt.Fprintln(os.Stderr, "Usage: camtool [globalopts] dump-diskpacked /path/to/diskpacked/directory")
}

func (c *dumpDpCmd) RunCommand(args []string) error {
	var path string

	switch len(args) {
	case 0:
	case 1:
		path = args[0]
	default:
		return errors.New("More than 1 argument not allowed")
	}
	cfg, err := serverinit.LoadFile(osutil.UserServerConfigPath())
	if err != nil {
		return err
	}
	prefixes, ok := cfg.Obj["prefixes"].(map[string]interface{})
	if !ok {
		return fmt.Errorf("No 'prefixes' object in low-level (or converted) config file %s", osutil.UserServerConfigPath())
	}
	paths := []string{}
	for prefix, vei := range prefixes {
		pmap, ok := vei.(map[string]interface{})
		if !ok {
			log.Printf("prefix %q value is a %T, not an object", prefix, vei)
			continue
		}
		pconf := jsonconfig.Obj(pmap)
		handlerType := pconf.RequiredString("handler")
		handlerArgs := pconf.OptionalObject("handlerArgs")
		// no pconf.Validate, as this is a recover tool
		if handlerType != "storage-diskpacked" {
			continue
		}
		log.Printf("handlerArgs of %q: %v", prefix, handlerArgs)
		if handlerArgs == nil {
			log.Printf("no handlerArgs for %q", prefix)
			continue
		}
		aconf := jsonconfig.Obj(handlerArgs)
		apath := aconf.RequiredString("path")
		// no aconv.Validate, as this is a recover tool
		if apath == "" {
			log.Printf("path is missing for %q", prefix)
			continue
		}
		if path != "" && path != apath {
			continue
		}
		paths = append(paths, apath)
	}
	if len(paths) == 0 {
		return fmt.Errorf("Server config file %s doesn't specify a disk-packed storage handler.",
			osutil.UserServerConfigPath())
	}
	if len(paths) > 1 {
		return fmt.Errorf("Ambiguity. Server config file %s d specify more than 1 disk-packed storage handler. Please specify one of: %v", osutil.UserServerConfigPath(), paths)
	}
	path = paths[0]
	if path == "" {
		return errors.New("no path is given/found")
	}

	var out io.WriteCloser
	if c.output == "" || c.output == "-" {
		out = os.Stdout
	} else {
		if out, err = os.Create(c.output); err != nil {
			return err
		}
	}
	err = diskpacked.Dump(out, path)
	if closeErr := out.Close(); closeErr != nil {
		log.Printf("error closing %q: %v", c.output, closeErr)
		if err == nil {
			err = closeErr
		}
	}
	return err
}
