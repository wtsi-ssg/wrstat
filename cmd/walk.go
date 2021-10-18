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
	"path/filepath"
	"syscall"

	"github.com/VertebrateResequencing/wr/jobqueue"
	"github.com/karrick/godirwalk"
	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/scheduler"
)

const defaultInodesPerJob = 2000000

// options for this cmd.
var outputDir string
var depGroup string
var walkInodesPerJob int
var walkID string
var walkCh string

// walkCmd represents the walk command.
var walkCmd = &cobra.Command{
	Use:   "walk",
	Short: "Walk a directory to get all its contents",
	Long: `Walk a directory to get all its contents.

wr manager must have been started before running this. If the manager can run
commands on multiple nodes, be sure to set wr's ManagerHost config option to
the host you started the manager on. Or run this from the same node that you
started the manager on.

For full access to all files, either start wr manager as root, or start it as a
user that can sudo without a password when running wrstat, and supply the --sudo
option to this command.

For each entry recursively within the directory of interest, their paths are
written to output files in the given output directory. The number of files is
such that they will each contain about --inodes_per_stat entries.

For each output file, a 'wrstat stat' job is then added to wr's queue with the
given dependency group. For the meaning of the --ch option which is passed
through to stat, see 'wrstat stat -h'.

(When jobs are added to wr's queue to get the work done, they are given a
--rep_grp of wrstat-stat-[id], so you can use
'wr status -i wrstat-stat -z -o s' to get information on how long everything or
particular subsets of jobs took.)

NB: when this exits, that does not mean all stats have necessarily been
retrieved. You should wait until all jobs in the given dependency group have
completed (eg. by adding your own job that depends on that group, such as a
'wrstat combine' call).`,
	Run: func(cmd *cobra.Command, args []string) {
		desiredDir := checkArgs(outputDir, depGroup, args)

		s, d := newScheduler("")
		defer d()

		if walkID == "" {
			walkID = statRepGrp(desiredDir, scheduler.UniqueString())
		}

		walkDirAndScheduleStats(desiredDir, outputDir, walkInodesPerJob, depGroup, walkID, walkCh, s)
	},
}

func init() {
	RootCmd.AddCommand(walkCmd)

	// flags specific to this sub-command
	walkCmd.Flags().IntVarP(&walkInodesPerJob, "inodes_per_stat", "n",
		defaultInodesPerJob, "number of inodes each parallel stat job will run on")
	walkCmd.Flags().StringVarP(&outputDir, "output_directory", "o", "", "base directory for output files")
	walkCmd.Flags().StringVarP(&walkID,
		"id", "i", "",
		"rep_grp suffix when adding jobs (default [directory_basename]-[date]-[unique])")
	walkCmd.Flags().StringVarP(
		&depGroup,
		"dependency_group", "d", "",
		"dependency group that stat jobs added to wr will belong to")
	walkCmd.Flags().StringVar(&walkCh, "ch", "", "passed through to 'wrstat stat'")
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

// statRepGrp returns a rep_grp that can be used for the stat jobs walk will
// create.
func statRepGrp(dir, unique string) string {
	return repGrp("stat", dir, unique)
}

// walkDirAndScheduleStats does the main work.
func walkDirAndScheduleStats(desiredDir, outputDir string, inodes int, depGroup, repGroup,
	yamlPath string, s *scheduler.Scheduler) {
	outPaths := writeAllPathsToFiles(desiredDir, outputDir, inodes)
	scheduleStatJobs(outPaths, depGroup, repGroup, yamlPath, s)
}

// writeAllPathsToFiles quickly traverses the entire file tree, writing out
// paths it encounters split over n files. It returns the paths to the output
// files created.
func writeAllPathsToFiles(desiredDir, outputDir string, inodes int) []string {
	n := calculateSplitBasedOnInodes(inodes, desiredDir)
	outPaths := make([]string, n)

	files := make([]*os.File, n)
	for i := range files {
		files[i] = createWalkOutputFile(outputDir, i)
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

// calculateSplitBasedOnInodes sees how many used inodes are on the given path
// and provides the number of jobs such that each job would do inodes paths.
func calculateSplitBasedOnInodes(n int, mount string) int {
	var statfs syscall.Statfs_t
	if err := syscall.Statfs(mount, &statfs); err != nil {
		die("failed to stat the filesystem: %s", err)
	}

	inodes := statfs.Files - statfs.Ffree

	jobs := int(inodes) / n

	if jobs == 0 {
		jobs = 1
	}

	return jobs
}

// createWalkOutputFile creates an output file named 'walk.n' within the
// out directory. Creates out directory if necessary.
func createWalkOutputFile(out string, n int) *os.File {
	if err := os.MkdirAll(out, userOnlyPerm); err != nil {
		die("failed to create output directory: %s", err)
	}

	outFile, err := os.Create(filepath.Join(out, fmt.Sprintf("walk.%d", n)))
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
// The jobs are added with the given dep and rep groups, and the given yaml for
// the --ch arg if not blank.
func scheduleStatJobs(outPaths []string, depGroup string, repGrp, yamlPath string, s *scheduler.Scheduler) {
	jobs := make([]*jobqueue.Job, len(outPaths))

	cmd := fmt.Sprintf("%s stat ", s.Executable())
	if yamlPath != "" {
		cmd += fmt.Sprintf("--ch %s ", yamlPath)
	}

	for i, path := range outPaths {
		jobs[i] = s.NewJob(cmd+path, repGrp, "wrstat-stat", depGroup, "")
	}

	addJobsToQueue(s, jobs)
}
