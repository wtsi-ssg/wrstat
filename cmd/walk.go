/*******************************************************************************
 * Copyright (c) 2021, 2022 Genome Research Ltd.
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
	"strconv"
	"syscall"
	"time"

	"github.com/VertebrateResequencing/wr/client"
	"github.com/VertebrateResequencing/wr/jobqueue"
	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/v6/walk"
)

const (
	defaultInodesPerJob   = 1000000
	walkLogOutputBasename = "walk.log"
	statTime              = 3 * time.Hour
	statRAM               = 200
	statCores             = 0.05
	timeoutExitCode       = 128
)

// options for this cmd.
var (
	outputDir        string
	depGroup         string
	walkInodesPerJob int
	walkNumOfJobs    int
	walkID           string
	walkCh           string
)

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
quickly retrieved (without doing any expensive stat calls) and written (quoted)
to output files in the given output directory. The number of files is such that
they will each contain about --inodes_per_stat entries (or if --num_stats was
supplied greater than zero, then there will be that number of output files).

For each output file, a 'wrstat stat' job is then added to wr's queue with the
given dependency group. For the meaning of the --ch option which is passed
through to stat, see 'wrstat stat -h'.

(When jobs are added to wr's queue to get the work done, they are given a
--rep_grp of wrstat-stat-[id], so you can use
'wr status -i wrstat-stat -z -o s' to get information on how long everything or
particular subsets of jobs took.)

NB: when this exits, that does not mean stats have been retrieved. You should
wait until all jobs in the given dependency group have completed (eg. by adding
your own job that depends on that group, such as a 'wrstat combine' call).`,
	Run: func(cmd *cobra.Command, args []string) {
		desiredDir := checkArgs(outputDir, depGroup, args)

		s, d := newScheduler("", forcedQueue, queuesToAvoid, sudo)
		defer d()

		if walkID == "" {
			walkID = statRepGrp(desiredDir, client.UniqueString(), time.Now().Format("20060102-150405"))
		}

		walkDirAndScheduleStats(desiredDir, outputDir, walkNumOfJobs, walkInodesPerJob, maximumAverageStatTime,
			depGroup, walkID, walkCh, s)
	},
}

func init() {
	RootCmd.AddCommand(walkCmd)

	// flags specific to this sub-command
	walkCmd.Flags().IntVarP(&walkInodesPerJob, "inodes_per_stat", "n",
		defaultInodesPerJob, "number of inodes each parallel stat job will run on")
	walkCmd.Flags().IntVarP(&walkNumOfJobs, "num_stat_jobs", "j",
		0, "force a specific number of parallel stat jobs (ignore -n if above 0)")
	walkCmd.Flags().StringVarP(&outputDir, "output_directory", "o", "", "base directory for output files")
	walkCmd.Flags().StringVarP(&walkID,
		"id", "i", "",
		"rep_grp suffix when adding jobs (default [directory_basename]-[date]-[unique])")
	walkCmd.Flags().StringVarP(
		&depGroup,
		"dependency_group", "d", "",
		"dependency group that stat jobs added to wr will belong to")
	walkCmd.Flags().StringVar(&walkCh, "ch", "", "passed through to 'wrstat stat'")
	walkCmd.Flags().StringVarP(&forcedQueue, "queue", "q", "", "force a particular queue to be used when scheduling jobs")
	walkCmd.Flags().StringVar(&queuesToAvoid, "queues_avoid", "",
		"force queues that include a substring from this comma-separated list to be avoided when scheduling jobs")
	walkCmd.Flags().Int64VarP(&timeout, "timeout", "t", 0, "stat jobs should start running before this unix timestamp")
	walkCmd.Flags().Int64VarP(&recordStats, "syscalls", "s", 0, "record "+
		"statistics on syscalls every n minutes to the log")
	walkCmd.Flags().BoolVarP(&statBlockSize, "blocks", "b", false, "record "+
		"disk usage (blocks) instead of apparent byte size")
	walkCmd.Flags().IntVarP(&maximumAverageStatTime, "maximum_stat_time", "M", defaultMaximumAveerageStatTime,
		"Maxiumum average stat time (seconds); will fail if the average (over 1000 stats) goes above this number")
}

// checkArgs checks we have required args and returns desired dir.
func checkArgs(out, dep string, args []string) string {
	if out == "" {
		die("--output_directory is required")
	}

	if dep == "" {
		die("--dependency_group is required")
	}

	if len(args) != 1 {
		die("exactly 1 directory of interest must be supplied")
	}

	return args[0]
}

// statRepGrp returns a rep_grp that can be used for the stat jobs walk will
// create.
func statRepGrp(dir, unique, now string) string {
	return repGrp("stat", dir, unique, now)
}

// walkDirAndScheduleStats does the main work.
func walkDirAndScheduleStats(desiredDir, outputDir string, statJobs, inodes, maximumAverageStatTime int, //nolint:funlen
	depGroup, repGroup, yamlPath string, s *client.Scheduler,
) {
	n := statJobs
	if n == 0 {
		n = calculateSplitBasedOnInodes(inodes, desiredDir)
	}

	files, err := walk.NewFiles(outputDir, n)
	if err != nil {
		die("failed to create walk output files: %s", err)
	}

	logToFile(filepath.Join(outputDir, walkLogOutputBasename))

	go keepAliveCheck(outputDir, "output directory no longer exists")

	walker := walk.New(files.WritePaths(), true, false)

	defer func() {
		err = files.Close()
		if err != nil {
			warn("failed to close walk output file: %s", err)
		}

		if recordStats > 0 {
			appLogger.Info("syscalls", "time", time.Now(), "writes", walker.Write, "writeBytes", walker.WriteBytes)
		}
	}()

	if recordStats > 0 {
		host, errr := os.Hostname()
		if errr != nil {
			die("failed to get hostname: %s", errr)
		}

		appLogger.Info("syscall logging", "host", host)

		walker.EnableStats(time.Duration(recordStats)*time.Minute, func(t time.Time, sd walk.StatData) {
			appLogger.Info("syscalls", "time", t, "opens", sd.Open, "reads", sd.Read,
				"bytes", sd.Bytes, "closes", sd.Close, "stats", sd.Stat,
				"writes", sd.Write, "writeBytes", sd.WriteBytes)
		})

		files.SetLogger(walker.AddWrite)
	}

	err = walker.Walk(desiredDir, func(path string, err error) {
		warn("error processing %s: %s", path, err)
	})
	if err != nil {
		die("failed to walk the filesystem: %s", err)
	}

	scheduleStatJobs(files.Paths, depGroup, repGroup, yamlPath, maximumAverageStatTime, s)
}

func keepAliveCheck(required, msg string) {
	for {
		time.Sleep(time.Minute)

		if _, err := os.Stat(required); err != nil {
			appLogger.Error(msg)
			os.Exit(timeoutExitCode)
		}
	}
}

// calculateSplitBasedOnInodes sees how many used inodes are on the given path
// and provides the number of jobs such that each job would do inodes paths.
func calculateSplitBasedOnInodes(n int, mount string) int {
	var statfs syscall.Statfs_t
	if err := syscall.Statfs(mount, &statfs); err != nil {
		die("failed to stat the filesystem at %s: %s", mount, err)
	}

	inodes := statfs.Files - statfs.Ffree

	jobs := int(inodes) / n

	if jobs == 0 {
		jobs = 1
	}

	return jobs
}

// scheduleStatJobs adds a 'wrstat stat' job to wr's queue for each out path.
// The jobs are added with the given dep and rep groups, and the given yaml for
// the --ch arg if not blank.
func scheduleStatJobs(outPaths []string, depGroup string, //nolint:funlen
	repGrp, yamlPath string, maximumAverageStatTime int, s *client.Scheduler,
) {
	jobs := make([]*jobqueue.Job, len(outPaths))

	cmd := s.Executable() + " stat "
	if yamlPath != "" {
		cmd += fmt.Sprintf("--ch %s ", yamlPath)
	}

	if recordStats > 0 {
		cmd += fmt.Sprintf("-s %d ", recordStats)
	}

	if statBlockSize {
		cmd += "-b "
	}

	if maximumAverageStatTime != defaultMaximumAveerageStatTime {
		cmd += "-M " + strconv.Itoa(maximumAverageStatTime)
	}

	req := client.DefaultRequirements()
	req.Time = statTime
	req.RAM = statRAM
	req.Cores = statCores
	limitGroups := []string{"wrstat-stat"}

	if timeout > 0 {
		limitGroups = append(limitGroups, "datetime<"+time.Unix(timeout, 0).Format(time.DateTime))
	}

	for i, path := range outPaths {
		jobs[i] = s.NewJob(cmd+path, repGrp, "wrstat-stat", depGroup, "", req)
		jobs[i].LimitGroups = limitGroups
	}

	addJobsToQueue(s, jobs)
}
