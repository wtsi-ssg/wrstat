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

package cmd

import (
	"fmt"
	"os"
	"time"

	"github.com/VertebrateResequencing/wr/jobqueue"
	"github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	"github.com/karrick/godirwalk"
	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/hashdir"
)

// options for this cmd.
var outputDir string
var depGroup string
var nJobs int

const jobRetries uint8 = 3
const reqRAM = 50
const reqTime = 2 * time.Second
const reqCores = 1
const reqDiesk = 1

var req = &scheduler.Requirements{
	RAM:   reqRAM,
	Time:  reqTime,
	Cores: reqCores,
	Disk:  reqDiesk,
}

// walkCmd represents the walk command.
var walkCmd = &cobra.Command{
	Use:   "walk",
	Short: "Walk a directory to get all its contents",
	Long: `Walk a directory to get all its contents.

wr manager must have been started before running this. If the manager can run
commands on multiple nodes, be sure to set wr's ManagerHost config option to
the host you started the manager on. Or run this from the same node that you
started the manager on.

For each entry recursively within the directory of interest, their paths are
written to --parallel_jobs output files (stored in hashed subdirectories of the
given output directory).

For each output file, a 'wrstat stat' job is then added to wr's queue with the
given dependency group.

NB: when this exits, that does not mean all stats have necessarily been
retrieved. You should wait until all jobs in the given dependency group have
completed (eg. by adding your own job that depends on that group, such as a
'wrstat combine' call).`,
	Run: func(cmd *cobra.Command, args []string) {
		desiredDir := checkArgs(outputDir, depGroup, args)

		jq, err := jobqueue.ConnectUsingConfig(deployment, connectTimeout, appLogger)
		if err != nil {
			die("could not connect to the wr manager: %s", err)
		}
		defer func() {
			err = jq.Disconnect()
		}()

		walkDirAndScheduleStats(desiredDir, outputDir, nJobs, depGroup, jq)
	},
}

func init() {
	RootCmd.AddCommand(walkCmd)

	// flags specific to this sub-command
	walkCmd.Flags().IntVarP(&nJobs, "parallel_jobs", "n", 64, "number of parallel jobs to run at once")
	walkCmd.Flags().StringVarP(&outputDir, "output_directory", "o", "", "base directory for output files")
	walkCmd.Flags().StringVarP(
		&depGroup,
		"dependency_group", "d", "",
		"dependency group that stat jobs added to wr will belong to")
}

// checkArgs checks we have required args and returns desired dir.
func checkArgs(out, dep string, args []string) string {
	if out == "" {
		die("--output_directory is required")
	}

	if dep == "" {
		die("--dependecy_group is required")
	}

	if len(args) != 1 {
		die("exactly 1 directory of interest must be supplied")
	}

	return args[0]
}

// walkDirAndScheduleStats does the main work.
func walkDirAndScheduleStats(desiredDir, outputDir string, n int, depGroup string, jq *jobqueue.Client) {
	outPaths := writeAllPathsToFiles(desiredDir, outputDir, n)
	scheduleStatJobs(outPaths, depGroup, jq)
}

// writeAllPathsToFiles quickly traverses the entire file tree, writing out
// paths it encounters split over n files. It returns the paths to the output
// files created.
func writeAllPathsToFiles(desiredDir, outputDir string, n int) []string {
	outPaths := make([]string, n)

	files := make([]*os.File, n)
	for i := range files {
		files[i] = createWalkOutputFile(outputDir, desiredDir, i)
		defer func(num int) {
			err := files[num].Close()
			if err != nil {
				warn("failed to close file: %s", err)
			}
		}(i)

		outPaths[i] = files[i].Name()
	}

	err := walkDir(desiredDir, files)
	if err != nil {
		die("failed to walk the filesystem: %s", err)
	}

	return outPaths
}

// createWalkOutputFile creates an output file within out in hashed location
// based on desired. It appends the given int to the filename.
func createWalkOutputFile(out, desired string, n int) *os.File {
	h := hashdir.New(hashdir.RecommendedLevels)

	outFile, err := h.CreateFileInHashedDir(out, desired, fmt.Sprintf(".%d", n))
	if err != nil {
		die("failed to create output file: %s", err)
	}

	return outFile
}

// walkDir walks the file system and writes every path encountered split across
// the given files.
func walkDir(desiredDir string, files []*os.File) error {
	i := 0
	max := len(files)

	return godirwalk.Walk(desiredDir, &godirwalk.Options{
		Callback: func(path string, de *godirwalk.Dirent) error {
			_, err := files[i].WriteString(path + "\n")
			i++
			if i == max {
				i = 0
			}

			return err
		},
		Unsorted: true,
	})
}

// scheduleStatJobs adds a 'wrstat stat' job to wr's queue for each out path.
// The jobs are added with the given dep group.
func scheduleStatJobs(outPaths []string, depGroup string, jq *jobqueue.Client) {
	cwd, err := os.Getwd()
	if err != nil {
		die("failed to get working directory: %s", err)
	}

	exe, err := os.Executable()
	if err != nil {
		die("failed to get wrstat's path: %s", err)
	}

	depGroups := []string{depGroup}
	jobs := make([]*jobqueue.Job, len(outPaths))

	for i, path := range outPaths {
		jobs[i] = &jobqueue.Job{
			Cmd:          fmt.Sprintf("%s stat %s", exe, path),
			Cwd:          cwd,
			CwdMatters:   true,
			RepGroup:     "wrstat-stat",
			ReqGroup:     "wrstat-stat",
			Requirements: req,
			DepGroups:    depGroups,
			Retries:      jobRetries,
		}
	}

	addJobsToQueue(jq, jobs)
}

// addJobsToQueue adds the jobs to wr's queue.
func addJobsToQueue(jq *jobqueue.Client, jobs []*jobqueue.Job) {
	inserts, dups, err := jq.Add(jobs, os.Environ(), false)
	if err != nil {
		die("failed to add jobs to wr's queue: %s", err)
	}

	if inserts != len(jobs) {
		warn("not all jobs were added to wr's queue; %d were duplicates", dups)
	}
}
