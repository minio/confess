// Copyright (c) 2022 MinIO, Inc.
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"syscall"
	"time"

	"github.com/cheggaaa/pb"
	"github.com/minio/cli"
	"github.com/minio/pkg/console"
)

var (
	version                     = "1.0.0"
	globalContext, globalCancel = context.WithCancel(context.Background())
	globalTermWidth             = 120

	// number of concurrent workers
	concurrency = 100
)

const (
	envPrefix = "CONFESS_"
)

func main() {
	app := cli.NewApp()
	app.Name = os.Args[0]
	app.Author = "MinIO, Inc."
	app.Description = `Object store consistency checker`
	app.UsageText = "HOSTS [FLAGS]"
	app.Version = version
	app.Copyright = "(c) 2022 MinIO, Inc."
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "access-key",
			Usage:  "Specify access key",
			EnvVar: envPrefix + "ACCESS_KEY",
			Value:  "",
		},
		cli.StringFlag{
			Name:   "secret-key",
			Usage:  "Specify secret key",
			EnvVar: envPrefix + "SECRET_KEY",
			Value:  "",
		},
		cli.BoolFlag{
			Name:  "insecure",
			Usage: "disable SSL certificate verification",
		},
		cli.StringFlag{
			Name:   "region",
			Usage:  "Specify a custom region",
			EnvVar: envPrefix + "REGION",
		},
		cli.StringFlag{
			Name:   "signature",
			Usage:  "Specify a signature method. Available values are S3V2, S3V4",
			Value:  "S3V4",
			Hidden: true,
		},
		cli.StringFlag{
			Name:  "bucket",
			Usage: "Bucket to use for confess tests",
		},
		cli.StringFlag{
			Name:  "output, o",
			Usage: "Specify output path for confess log",
		},
	}
	app.CustomAppHelpTemplate = `NAME:
  {{.Name}} - {{.Description}}
USAGE:
  {{.Name}} - {{.UsageText}}
HOSTS:
  HOSTS is a comma separated list or a range of hostnames/ip-addresses
FLAGS:
  {{range .VisibleFlags}}{{.}}
  {{end}}
EXAMPLES:
  1. Run consistency across 4 MinIO Servers (http://minio1:9000 to http://minio4:9000)
     $ confess --access-key minio --secret-key minio123 -o /tmp/dir http://minio{1...4}:9000 
`
	app.Action = confessMain
	app.Run(os.Args)
}

func checkMain(ctx *cli.Context) {
	if !ctx.Args().Present() {
		console.Fatalln(fmt.Errorf("not arguments found, please check documentation '%s --help'", ctx.App.Name))
	}
	if ctx.String("bucket") == "" {
		console.Fatalln("--bucket flag needs to be set")
	}
	if !ctx.IsSet("access-key") || !ctx.IsSet("secret-key") {
		console.Fatalln("--access-key and --secret-key flags needs to be set")
	}
}

func confessMain(ctx *cli.Context) {
	checkMain(ctx)
	rand.Seed(time.Now().UnixNano())
	nodeState := newNodeState(ctx)
	nodeState.init(globalContext)
	console.Println(whiteStyle.Render("confess " + version + "\nCopyright 2022 MinIO\nGNU AGPL V3\n"))

	// set terminal size if available.
	if w, e := pb.GetTerminalWidth(); e == nil {
		globalTermWidth = w
	}
	// Monitor OS exit signals and cancel the global context in such case
	go nodeState.trapSignals(os.Interrupt, syscall.SIGTERM, syscall.SIGKILL)
	go func() {
		e := nodeState.runTests(globalContext)
		if e != nil && !errors.Is(e, context.Canceled) {
			console.Fatalln(fmt.Errorf("unable to run confess: %w", e))
		}
	}()
	nodeState.finish(globalContext)
}
