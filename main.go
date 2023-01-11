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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/cheggaaa/pb"
	"github.com/fatih/color"
	"github.com/minio/cli"
	"github.com/minio/pkg/console"
)

var (
	globalContext, globalCancel = context.WithCancel(context.Background())
	globalTermWidth             = 120

	// number of concurrent workers
	concurrency = 100
)

const (
	envPrefix = "CONFESS_"
)

var buildInfo = map[string]string{}

func init() {
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, skv := range bi.Settings {
			buildInfo[skv.Key] = skv.Value
		}
	}
}

func main() {
	cli.VersionPrinter = func(c *cli.Context) {
		io.Copy(c.App.Writer, versionBanner(c))
	}

	app := cli.NewApp()
	app.Name = os.Args[0]
	app.Author = "MinIO, Inc."
	app.Description = `Object store consistency checker`
	app.UsageText = "[FLAGS] HOSTS"
	app.Copyright = "(c) 2022 MinIO, Inc."
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "access-key",
			Usage:  "specify access key",
			EnvVar: envPrefix + "ACCESS_KEY",
			Value:  "",
		},
		cli.StringFlag{
			Name:   "secret-key",
			Usage:  "specify secret key",
			EnvVar: envPrefix + "SECRET_KEY",
			Value:  "",
		},
		cli.BoolFlag{
			Name:   "insecure",
			Usage:  "disable TLS certificate verification",
			EnvVar: envPrefix + "INSECURE",
		},
		cli.StringFlag{
			Name:   "region",
			Usage:  "specify a custom region",
			EnvVar: envPrefix + "REGION",
		},
		cli.StringFlag{
			Name:   "signature",
			Usage:  "Specify a signature method. Supported values are s3v2, s3v4",
			Value:  "s3v4",
			EnvVar: envPrefix + "SIGNATURE",
			Hidden: true,
		},
		cli.StringFlag{
			Name:   "bucket",
			Usage:  "Bucket to use for confess tests",
			EnvVar: envPrefix + "BUCKET",
		},
		cli.StringFlag{
			Name:  "output, o",
			Usage: "specify output path for confess log",
		},
		cli.StringFlag{
			Name:  "duration, d",
			Usage: "Duration to run the tests. Use 's' and 'm' to specify seconds and minutes.",
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
     $ confess --access-key minio --secret-key minio123 http://minio{1...4}:9000
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

func startupBanner(banner io.Writer) {
	fmt.Fprintln(banner, Blue("Copyright:")+Bold(" 2022 MinIO, Inc."))
	fmt.Fprintln(banner, Blue("License:")+Bold(" GNU AGPLv3 <https://www.gnu.org/licenses/agpl-3.0.html>"))
	fmt.Fprintln(banner, Blue("Version:")+Bold(" %s (%s %s/%s)", getVersion(), runtime.Version(), runtime.GOOS, runtime.GOARCH))
}

func versionBanner(c *cli.Context) io.Reader {
	banner := &strings.Builder{}
	fmt.Fprintln(banner, Bold("%s version %s (commit-id=%s)", c.App.Name, getVersion(), getRevision()))
	fmt.Fprintln(banner, Blue("Runtime:")+Bold(" %s %s/%s", runtime.Version(), runtime.GOOS, runtime.GOARCH))
	fmt.Fprintln(banner, Blue("License:")+Bold(" GNU AGPLv3 <https://www.gnu.org/licenses/agpl-3.0.html>"))
	fmt.Fprintln(banner, Blue("Copyright:")+Bold(" 2022 MinIO, Inc."))
	return strings.NewReader(banner.String())
}

func getVersion() string {
	return strings.ReplaceAll(buildInfo["vcs.time"], ":", "-")
}

func getRevision() string {
	return buildInfo["vcs.revision"]
}

func confessMain(ctx *cli.Context) {
	checkMain(ctx)
	console.SetColor("metrics-duration", color.New(color.FgHiWhite))
	console.SetColor("metrics-title", color.New(color.FgCyan))
	console.SetColor("metrics-zero", color.New(color.FgHiWhite))

	rand.Seed(time.Now().UnixNano())
	nodeState := newNodeState(ctx)
	nodeState.init(globalContext)

	var builder bytes.Buffer
	startupBanner(&builder)
	console.Println(builder.String())

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
