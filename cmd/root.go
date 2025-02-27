/*******************************************************************************
 * Copyright (c) 2021 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

// package cmd is the cobra file that enables subcommands and handles
// command-line args.

package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/VertebrateResequencing/wr/client"
	"github.com/VertebrateResequencing/wr/jobqueue"
	"github.com/inconshreveable/log15"
	"github.com/spf13/cobra"
)

const userGroupPerm = 0770

// appLogger is used for logging events in our commands.
var appLogger = log15.New()

// these variables are accessible by all subcommands.
var (
	deployment string
	sudo       bool
)

const connectTimeout = 10 * time.Second

// RootCmd represents the base command when called without any subcommands.
var RootCmd = &cobra.Command{
	Use:   "wrstat",
	Short: "wrstat gets stats on all files in a filesystem directory tree.",
	Long: `wrstat gets stats on all files in a filesystem directory tree.

It uses wr to queue getting the stats for subsets of the tree, so enabling the
work to be done in parallel and potentially distributed over many nodes.

Before doing anything else, the wr manager must be running. If the manager can
run commands on multiple nodes, be sure to set wr's ManagerHost config option to
the host you started the manager on. Or run commands from the same node that you
started the manager on.

If you need root to have permission to see all deseired files, either start wr
manager as root, or start it as a user that can sudo without a password when
running wrstat, and supply the --sudo option to wrstat sub commands.

For raw stats on a directory and all its sub contents:
$ wrstat walk -o [/output/location] -d [dependency_group] [/location/of/interest]

Combine all the above output files:
$ wrstat combine [/output/location]

Or more easily work on multiple locations of interest at once by doing the
above 2 steps on each location and moving the final results to a final location:
$ wrstat multi -w [/working/directory] -f [/final/output/dir] [/a /b /c]`,
}

// Execute adds all child commands to the root command and sets flags
// appropriately. This is called by main.main(). It only needs to happen once to
// the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		die("%s", err.Error())
	}
}

func init() {
	// set up logging to stderr
	appLogger.SetHandler(log15.LvlFilterHandler(log15.LvlInfo, log15.StderrHandler))

	// global flags
	RootCmd.PersistentFlags().StringVar(&deployment,
		"deployment",
		"production",
		"the deployment your wr manager was started with")

	RootCmd.PersistentFlags().BoolVar(&sudo,
		"sudo",
		false,
		"created jobs will run with sudo")
}

// logToFile logs to the given file.
func logToFile(path string) {
	fh, err := log15.FileHandler(path, log15.LogfmtFormat())
	if err != nil {
		warn("Could not log to file [%s]: %s", path, err)

		return
	}

	appLogger.SetHandler(fh)
}

// info is a convenience to log a message at the Info level.
func info(msg string, a ...interface{}) {
	appLogger.Info(fmt.Sprintf(msg, a...))
}

// warn is a convenience to log a message at the Warn level.
func warn(msg string, a ...interface{}) {
	appLogger.Warn(fmt.Sprintf(msg, a...))
}

// die is a convenience to log a message at the Error level and exit non zero.
func die(msg string, a ...interface{}) {
	appLogger.Error(fmt.Sprintf(msg, a...))
	os.Exit(1)
}

// newScheduler returns a new Scheduler, exiting on error. It also returns a
// function you should defer.
//
// If you provide a non-blank queue, that queue will be used when scheduling. If
// you provide a non-black queuesAvoid, queues including a substring from the
// list will be avoided.
func newScheduler(cwd, queue, queuesAvoid string, sudo bool) (*client.Scheduler, func()) {
	s, err := client.New(client.SchedulerSettings{
		Deployment: deployment, Cwd: cwd,
		Queue:       queue,
		QueuesAvoid: queuesAvoid,
		Timeout:     connectTimeout,
		Logger:      appLogger})
	if err != nil {
		die("%s", err)
	}

	if sudo {
		s.EnableSudo()
	}

	return s, func() {
		err = s.Disconnect()
		if err != nil {
			warn("failed to disconnect from wr manager: %s", err)
		}
	}
}

// repGrp returns a rep_grp that can be used for a wrstat job we will create.
func repGrp(cmd, dir, unique, now string) string {
	return fmt.Sprintf("wrstat-%s-%s-%s-%s", cmd, dir, now, unique)
}

// addJobsToQueue adds the jobs to wr's queue.
func addJobsToQueue(s *client.Scheduler, jobs []*jobqueue.Job) {
	if err := s.SubmitJobs(jobs); err != nil {
		die("failed to add jobs to wr's queue: %s", err)
	}
}
