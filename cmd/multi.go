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
	"strings"
	"time"

	"github.com/VertebrateResequencing/wr/jobqueue"
	jqs "github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/v6/scheduler"
)

const (
	walkTime      = 19 * time.Hour
	walkRAM       = 16000
	combineTime   = 40 * time.Minute
	combineRAM    = 800
	defaultMaxRAM = 42000
)

// options for this cmd.
var (
	workDir       string
	finalDir      string
	multiInodes   int
	multiStatJobs int
	multiCh       string
	forcedQueue   string
	queuesToAvoid string
	maxMem        int
	timeout       int64
)

// multiCmd represents the multi command.
var multiCmd = &cobra.Command{
	Use:   "multi",
	Short: "Get stats on the contents of multiple directories",
	Long: `Get stats on the contents of multiple directories.
 
wr manager must have been started before running this. If the manager can run
commands on multiple nodes, be sure to set wr's ManagerHost config option to
the host you started the manager on.

For full access to all files, either start wr manager as root, or start it as a
user that can sudo without a password when running wrstat, and supply the --sudo
option to this command.

This calls 'wrstat walk' and 'wrstat combine' on each of the given directories
of interest. Their outputs go to a unique subdirectory of the given
--working_directory, which means you can start running this before a previous
run has completed on the same inputs, and there won't be conflicts.
It is best if all your directories of interest have different basenames, but
things will still work and not conflict if they don't. To ensure this, the
output directory for each directory of interest is a unique subdirectory of the
unique directory created for all of them.

(When jobs are added to wr's queue to get the work done, they are given a
--rep_grp of wrstat-[cmd]-[directory_basename]-[date]-[unique], so you can use
'wr status -i wrstat -z -o s' to get information on how long everything or
particular subsets of jobs took.)

Once everything has completed, the final output files are moved to the given
--final_output directory by 'wrstat tidy', with a name that includes the date
this command was started, the basename of the directory operated on, a unique
string per directory of interest, and a unique string for this call of multi:
[year][month][day]_[directory_basename]/[interest unique].[unique].[type]
eg. for 'wrstat multi -i foo -w /path/a -f /path/b /mnt/foo /mnt/bar /home/bar'
It might produce: 
/path/b/20210617_foo.clkdnfnd992nfksj1lld.c35m8359bnc8ni7dgphg.logs.gz
/path/b/20210617_foo.clkdnfnd992nfksj1lld.c35m8359bnc8ni7dgphg.stats.gz
/path/b/20210617_bar.f8bns3jkd92kds10k4ks.c35m8359bnc8ni7dgphg.logs.gz
/path/b/20210617_bar.f8bns3jkd92kds10k4ks.c35m8359bnc8ni7dgphg.stats.gz
/path/b/20210617_bar.d498vhsk39fjh129djg8.c35m8359bnc8ni7dgphg.logs.gz
/path/b/20210617_bar.d498vhsk39fjh129djg8.c35m8359bnc8ni7dgphg.stats.gz

The output files will be given the same user:group ownership and
user,group,other read & write permissions as the --final_output directory.

Finally, the unique subdirectory of --working_directory that was created is
deleted.`,
	Run: func(cmd *cobra.Command, args []string) {
		checkMultiArgs()
		err := doMultiScheduling(args, workDir, forcedQueue, queuesToAvoid, sudo)
		if err != nil {
			die("%s", err)
		}
	},
}

func init() {
	RootCmd.AddCommand(multiCmd)

	// flags specific to this sub-command
	multiCmd.Flags().StringVarP(&workDir, "working_directory", "w", "", "base directory for intermediate results")
	multiCmd.Flags().StringVarP(&finalDir, "final_output", "f", "", "final output directory")
	multiCmd.Flags().IntVarP(&multiInodes, "inodes_per_stat", "n",
		defaultInodesPerJob, "number of inodes per parallel stat job")
	multiCmd.Flags().IntVarP(&multiStatJobs, "num_stat_jobs", "j",
		0, "force a specific number of parallel stat jobs (ignore -n if above 0)")
	multiCmd.Flags().StringVar(&multiCh, "ch", "", "passed through to 'wrstat walk'")
	multiCmd.Flags().StringVar(&forcedQueue, "queue", "", "force a particular queue to be used when scheduling jobs")
	multiCmd.Flags().StringVar(&queuesToAvoid, "queues_avoid", "",
		"force queues that include a substring from this comma-separated list to be avoided when scheduling jobs")
	multiCmd.Flags().IntVarP(&maxMem, "max_mem", "m", defaultMaxRAM, "maximum MBs to reserve for any job")
	multiCmd.Flags().Int64VarP(&timeout, "timeout", "t", 0, "maximum number of hours to run")
	multiCmd.Flags().StringVarP(&logsDir, "logdir", "l", "", "when timeout is "+
		"reached, copy logs to a unique subdirectory of the supplied directory")
	multiCmd.Flags().StringVarP(&logJobs, "logjobs", "L", "", "when timeout is "+
		"reached, log job status to a unique file (YYYY-MM-DD_unique.log) in the supplied directory")
}

// checkMultiArgs ensures we have the required args for the multi sub-command.
func checkMultiArgs() {
	if workDir == "" {
		die("--working_directory is required")
	}

	if finalDir == "" {
		die("--final_output is required")
	}
}

// doMultiScheduling does the main work of the multi sub-command.
func doMultiScheduling(args []string, workDir, forcedQueue, queuesToAvoid string, sudo bool) error {
	s, d := newScheduler(workDir, forcedQueue, queuesToAvoid, sudo)
	defer d()

	unique := scheduler.UniqueString()
	outputRoot := filepath.Join(workDir, unique)

	err := os.MkdirAll(outputRoot, userGroupPerm)
	if err != nil {
		return err
	}

	scheduleWalkJobs(outputRoot, args, unique, finalDir, multiStatJobs,
		multiInodes, multiCh, forcedQueue, queuesToAvoid, s)

	if timeout > 0 {
		scheduleCleanupJob(s, timeout, outputRoot, unique, logsDir, logJobs)
	}

	return nil
}

// scheduleWalkJobs adds a 'wrstat walk' job to wr's queue for each desired
// path. The second scheduler is used to add combine jobs, which need a memory
// override.
func scheduleWalkJobs(outputRoot string, desiredPaths []string, unique, finalDirParent string, //nolint:funlen
	numStatJobs, inodesPerStat int, yamlPath, queue, queuesAvoid string, s *scheduler.Scheduler) {
	walkJobs := make([]*jobqueue.Job, len(desiredPaths))
	combineJobs := make([]*jobqueue.Job, len(desiredPaths))
	tidyJobs := make([]*jobqueue.Job, len(desiredPaths))
	cmd := buildWalkCommand(s, numStatJobs, inodesPerStat, yamlPath, queue, queuesAvoid)
	now := time.Now().Unix()
	reqWalk, reqCombine := reqs()

	var (
		limit           []string
		limitDate       int64
		removeAfterBury jobqueue.Behaviours
	)

	if timeout > 0 {
		removeAfterBury = jobqueue.Behaviours{{Do: jobqueue.Remove}}
	}

	if timeout > 0 {
		maxStart := time.Now().Add(time.Duration(timeout) * time.Hour)
		limitDate = maxStart.Unix()
		limit = []string{"datetime<" + maxStart.Format(time.DateTime)}
	}

	for i, path := range desiredPaths {
		walkUnique := scheduler.UniqueString()
		combineUnique := scheduler.UniqueString()
		outDir := filepath.Join(outputRoot, filepath.Base(path), walkUnique)
		finalDirName := fmt.Sprintf("%d_%s", now, encodePath(path))
		finalOutput := filepath.Join(finalDirParent, finalDirName)

		walkJobs[i] = s.NewJob(fmt.Sprintf("%s -d %s -t %d -o %s -i %s %s",
			cmd, walkUnique, limitDate, outDir, statRepGrp(path, unique), path),
			walkRepGrp(path, unique), "wrstat-walk", walkUnique, "", reqWalk)
		walkJobs[i].LimitGroups = limit
		walkJobs[i].Behaviours = removeAfterBury

		combineJobs[i] = s.NewJob(fmt.Sprintf("%s combine %q", s.Executable(), outDir),
			combineRepGrp(path, unique), "wrstat-combine", combineUnique, walkUnique, reqCombine)
		combineJobs[i].LimitGroups = limit
		combineJobs[i].Behaviours = removeAfterBury

		tidyJobs[i] = s.NewJob(fmt.Sprintf("%s tidy -f %q %q",
			s.Executable(), finalOutput, outDir),
			tidyRepGrp(path, unique), "wrstat-tidy", "", combineUnique, scheduler.DefaultRequirements())
		tidyJobs[i].Behaviours = removeAfterBury
	}

	addJobsToQueue(s, walkJobs)
	addJobsToQueue(s, combineJobs)
	addJobsToQueue(s, tidyJobs)
}

// buildWalkCommand builds a wrstat walk command line based on the given n,
// yaml path, queue, and if sudo is in effect.
func buildWalkCommand(s *scheduler.Scheduler, numStatJobs, inodesPerStat int,
	yamlPath, queue, queuesAvoid string) string {
	cmd := s.Executable() + " walk "

	if numStatJobs > 0 {
		cmd += fmt.Sprintf("-j %d ", numStatJobs)
	} else {
		cmd += fmt.Sprintf("-n %d ", inodesPerStat)
	}

	if yamlPath != "" {
		cmd += fmt.Sprintf("--ch %s ", yamlPath)
	}

	if queue != "" {
		cmd += fmt.Sprintf("--queue %s ", queue)
	}

	if queuesAvoid != "" {
		cmd += fmt.Sprintf("--queues_avoid %s ", queuesAvoid)
	}

	if sudo {
		cmd += "--sudo "
	}

	return cmd
}

func encodePath(path string) string {
	var sb strings.Builder

	for i := 0; i < len(path); i++ {
		switch path[i] {
		case '%':
			sb.WriteString("%25")
		case '/':
			sb.WriteString("%2F")
		default:
			sb.WriteByte(path[i])
		}
	}

	return sb.String()
}

// reqs returns Requirements suitable for walk and combine jobs.
func reqs() (*jqs.Requirements, *jqs.Requirements) {
	req := scheduler.DefaultRequirements()
	reqWalk := req.Clone()
	reqWalk.Time = walkTime
	reqWalk.RAM = min(walkRAM, maxMem)
	reqCombine := req.Clone()
	reqCombine.Time = combineTime
	reqCombine.RAM = min(combineRAM, maxMem)

	return reqWalk, reqCombine
}

// walkRepGrp returns a rep_grp that can be used for the walk jobs multi will
// create.
func walkRepGrp(dir, unique string) string {
	return repGrp("walk", dir, unique)
}

// combineRepGrp returns a rep_grp that can be used for the combine jobs multi
// will create.
func combineRepGrp(dir, unique string) string {
	return repGrp("combine", dir, unique)
}

// tidyRepGrp returns a rep_grp that can be used for the tidy jobs multi will
// create.
func tidyRepGrp(dir, unique string) string {
	return repGrp("tidy", dir, unique)
}

func scheduleCleanupJob(s *scheduler.Scheduler, timeout int64, outputRoot, jobUnique, logOutput, jobOutput string) {
	cmd := fmt.Sprintf("%s cleanup -w %q -j %q", s.Executable(), outputRoot, jobUnique)
	nowUnique := time.Now().Format(time.DateOnly) + "_" + jobUnique

	if logOutput != "" {
		cmd += fmt.Sprintf(" -l %q", filepath.Join(logOutput, nowUnique))
	}

	if jobOutput != "" {
		cmd += fmt.Sprintf(" -L %q", filepath.Join(jobOutput, nowUnique+".log"))
	}

	job := s.NewJob(cmd, "wrstat-cleanup", "wrstat-cleanup", "", "", scheduler.DefaultRequirements())
	job.LimitGroups = []string{time.Now().Add(time.Hour*time.Duration(timeout)).Format(time.DateTime) + "<datetime"}

	addJobsToQueue(s, []*jobqueue.Job{job})
}
