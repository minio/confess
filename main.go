// Copyright (c) 2023 MinIO, Inc.
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
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"syscall"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/fatih/color"
	"github.com/minio/cli"
	"github.com/minio/confess/tests"
	"github.com/minio/confess/utils"
	"github.com/minio/pkg/console"
)

var (
	globalContext, globalCancel = context.WithCancel(context.Background())
)

const (
	envPrefix = "CONFESS_"
)

var buildInfo = map[string]string{}

func init() {
	rand.Seed(time.Now().UnixNano())
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
	app.Copyright = "(c) 2023 MinIO, Inc."
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
		cli.BoolFlag{
			Name:   "use-signv2",
			Usage:  "Use s3v2 as the signature method",
			EnvVar: envPrefix + "USE_SIGNV2",
		},
		cli.StringFlag{
			Name:   "bucket",
			Usage:  "Bucket to use for confess tests",
			EnvVar: envPrefix + "BUCKET",
		},
		cli.StringFlag{
			Name:  "output, o",
			Usage: "specify output file for confess log",
		},
		cli.DurationFlag{
			Name:  "duration, d",
			Usage: "Duration to run the tests. Use 's' and 'm' to specify seconds and minutes.",
			Value: 30 * time.Minute,
		},
		cli.Int64Flag{
			Name:  "fail-after, f",
			Usage: "fail after n errors. Defaults to 100",
			Value: 100,
		},
		cli.IntFlag{
			Name:  "test-concurrency",
			Usage: "Number of concurrent threads for each test in the test suite",
			Value: 10,
		},
		cli.IntFlag{
			Name:  "test-objects-count",
			Usage: "Number of test objects for each test in the test suite",
			Value: 50,
		},
		cli.IntFlag{
			Name:  "verbosity",
			Usage: "Set the logging verbosity",
			Value: 0,
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
   1. Run consistency across 4 MinIO Servers (http://minio1:9000 to http://minio4:9000) on "mybucket".
      $ confess --access-key minio --secret-key minio123 --bucket "mybucket" --o /tmp/confess.out --duration 30m http://minio{1...4}:9000
`
	app.Action = confessMain
	app.Run(os.Args)
}

func versionBanner(c *cli.Context) io.Reader {
	banner := &strings.Builder{}

	version := strings.ReplaceAll(buildInfo["vcs.time"], ":", "-")
	revision := buildInfo["vcs.revision"]

	fmt.Fprintln(banner, color.HiWhiteString("%s version %s (commit-id=%s)", c.App.Name, version, revision))
	fmt.Fprintln(banner, color.BlueString("Runtime:")+color.HiWhiteString(" %s %s/%s", runtime.Version(), runtime.GOOS, runtime.GOARCH))
	fmt.Fprintln(banner, color.BlueString("License:")+color.HiWhiteString(" GNU AGPLv3 <https://www.gnu.org/licenses/agpl-3.0.html>"))
	fmt.Fprintln(banner, color.BlueString("Copyright:")+color.HiWhiteString(" 2022 MinIO, Inc."))
	return strings.NewReader(banner.String())
}

func checkMain(ctx *cli.Context) {
	if !ctx.Args().Present() {
		cli.ShowCommandHelp(ctx, ctx.Command.Name)
		os.Exit(1)
	}
	if ctx.String("bucket") == "" {
		console.Fatalln("--bucket flag needs to be set")
	}
	if !ctx.IsSet("access-key") || !ctx.IsSet("secret-key") {
		console.Fatalln("--access-key and --secret-key flags needs to be set")
	}
}

func confessMain(c *cli.Context) {
	checkMain(c)

	accessKey := c.String("access-key")
	secretKey := c.String("secret-key")
	insecure := c.Bool("insecure")
	failAfter := c.Int64("fail-after")
	region := c.String("region")
	useSignV2 := c.Bool("use-signv2")
	bucket := c.String("bucket")
	outputFileName := c.String("output")
	duration := c.Duration("duration")
	concurrency := c.Int("test-concurrency")
	objectsCount := c.Int("test-objects-count")
	verbosity := c.Int("verbosity")
	hosts := c.Args()

	outputFile, err := os.OpenFile(outputFileName, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		console.Fatalln("unable to open output file", err)
	}
	defer outputFile.Close()

	if _, err := outputFile.WriteString(
		fmt.Sprintf(
			"\n****Config****\nHosts: %s\nBucket: %s\nDuration: %s\n**************\n",
			hosts,
			bucket,
			duration.String(),
		),
	); err != nil {
		console.Fatalln("unable to write to output file", err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM, syscall.SIGSEGV)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		s := <-sigs
		console.Printf(color.RedString("\nExiting on signal %v; %#v\n\n", s.String(), s))
		cancel()
	}()

	executor, err := NewExecutor(ctx, Config{
		Hosts:        hosts,
		AccessKey:    accessKey,
		SecretKey:    secretKey,
		Insecure:     insecure,
		Region:       region,
		UseSignV2:    useSignV2,
		Bucket:       bucket,
		Duration:     duration,
		FailAfter:    failAfter,
		ObjectsCount: objectsCount,
		Concurrency:  concurrency,
		Logger:       utils.NewLogger(outputFile, verbosity),
	})
	if err != nil {
		console.Fatalln(err)
	}

	var wg sync.WaitGroup
	m := newProgressModel(executor.Stats, cancel)
	teaProgram := tea.NewProgram(m, tea.WithAltScreen(), tea.WithMouseCellMotion())
	wg.Add(1)
	go func() {
		defer wg.Done()
		if _, err := teaProgram.Run(); err != nil {
			fmt.Println("error running program:", err)
			os.Exit(1)
		}
	}()

	// Start monitoring nodes health.
	go executor.MonitorNodeHealth(ctx, teaProgram)

	// run tests.
	executor.ExecuteTests(ctx, []tests.Test{
		&tests.PutListTest{},
		&tests.PutStatTest{},
		&tests.PutGetTest{},
		// new tests can be added here...
	}, teaProgram)

	// tests completed.
	teaProgram.Send(notification{
		done: true,
	})

	// wait for completion.
	wg.Wait()

	if _, err := outputFile.WriteString(
		fmt.Sprintf("\n****Summary****\n%s\n**************", executor.Stats.String()),
	); err != nil {
		console.Fatalln("unable to write to output file", err)
	}

	return
}
