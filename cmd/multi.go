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
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/VertebrateResequencing/wr/client"
	"github.com/VertebrateResequencing/wr/jobqueue"
	jqs "github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/v6/neaten"
)

const (
	walkTime      = 3 * time.Hour
	walkRAM       = 16000
	combineTime   = 40 * time.Minute
	combineRAM    = 800
	defaultMaxRAM = 42000
)

//nolint:gochecknoglobals // options for this cmd.
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
	recordStats   int64
	statBlockSize bool
	multiRun      bool
)

var ErrFinalOutputRequired = errors.New("--final_output is required")

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

NB: When running with sudo that is configured to not pass through environmental
variables, you must have a wr config file, accessible from the working
directory, with ManagerHost, ManagerPort, and ManagerCertDomain set.

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

If an existing run is in progress, a new run will not start unless the
--multirun/-S flag is specified.

Once everything has completed, the final output files are moved to the given
--final_output directory by 'wrstat tidy', within a subdirectory named for the
start time of the command followed by an underscore and the path (with the
slashes replaced with a unicode equivalent):
YYMMDD-hhmmss_[path]/[file]
eg. for 'wrstat multi -i foo -w /path/a -f /path/b /mnt/foo /mnt/bar /home/bar'
It might produce: 
/path/b/20210617-200000_／mnt／foo/logs.gz
/path/b/20210617-200000_／mnt／foo/stats.gz
/path/b/20210617-200000_／mnt／bar/logs.gz
/path/b/20210617-200000_／mnt／bar/stats.gz
/path/b/20210617-200000_／home／bar/logs.gz
/path/b/20210617-200000_／home／bar/stats.gz

The output files will be given the same user:group ownership and
user,group,other read & write permissions as the --final_output directory.

Finally, the unique subdirectory of --working_directory that was created is
deleted.`,
	RunE: func(_ *cobra.Command, args []string) error {
		if err := checkMultiArgs(); err != nil {
			return err
		}

		return doMultiScheduling(args, workDir, forcedQueue, queuesToAvoid, maximumAverageStatTime, sudo)
	},
}

func init() {
	if !initCmds {
		return
	}

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
	multiCmd.Flags().Int64VarP(&recordStats, "syscalls", "s", 0, "record "+
		"statistics on syscalls every n minutes to the log")
	multiCmd.Flags().BoolVarP(&statBlockSize, "blocks", "b", false, "record "+
		"disk usage (blocks) instead of apparent byte size")
	multiCmd.Flags().IntVarP(&maximumAverageStatTime, "maximum_stat_time", "M", defaultMaximumAveerageStatTime,
		"Maxiumum average stat time (seconds); will fail if the average (over 1000 stats) goes above this number")
	multiCmd.Flags().BoolVarP(&multiRun, "multirun", "S", false, "allow multiple runs to occur at the same time")
}

// checkMultiArgs ensures we have the required args for the multi sub-command.
func checkMultiArgs() error {
	if workDir == "" {
		return ErrWorkDirRequired
	}

	if finalDir == "" {
		return ErrFinalOutputRequired
	}

	if logJobs == "" {
		logJobs = finalDir
	}

	if logsDir == "" {
		logsDir = finalDir
	}

	return nil
}

// doMultiScheduling does the main work of the multi sub-command.
func doMultiScheduling(paths []string, workDir, //nolint:gocognit,gocyclo,cyclop,funlen
	forcedQueue, queuesToAvoid string, maximumAverageStatTime int, sudo bool) error {
	s, d, err := newScheduler(workDir, forcedQueue, queuesToAvoid, sudo)
	if err != nil {
		return err
	}

	defer d()

	desiredPaths := make([]string, 0, len(paths))

	for _, path := range paths {
		absPath, errr := filepath.Abs(path)
		if errr != nil {
			return errr
		}

		if !multiRun { //nolint:nestif
			repGrp := tidyRepGrp(absPath, "", "")
			repGrp = strings.TrimSuffix(repGrp, "--")

			jobs, errrr := s.FindJobsByRepGroupPrefixAndState(repGrp, jobqueue.JobStateDependent)
			if errrr != nil {
				return errrr
			}

			if len(jobs) != 0 {
				warn("there is already a queued job for %s", absPath)

				continue
			}
		}

		desiredPaths = append(desiredPaths, absPath)
	}

	if len(desiredPaths) == 0 {
		return nil
	}

	unique := client.UniqueString()
	outputRoot := filepath.Join(workDir, unique)
	now := time.Now().Format("20060102-150405")

	if err = os.MkdirAll(outputRoot, userGroupPerm); err != nil {
		return err
	}

	if err := scheduleWalkJobs(outputRoot, desiredPaths, unique, finalDir, multiStatJobs,
		multiInodes, maximumAverageStatTime, multiCh, forcedQueue, queuesToAvoid, now, s); err != nil {
		return err
	}

	if timeout > 0 {
		if err := scheduleCleanupJob(s, timeout, outputRoot, unique, logsDir, logJobs, now); err != nil {
			return err
		}
	}

	return nil
}

// scheduleWalkJobs adds a 'wrstat walk' job to wr's queue for each desired
// path. The second scheduler is used to add combine jobs, which need a memory
// override.
func scheduleWalkJobs(outputRoot string, desiredPaths []string, unique, finalDirParent string, //nolint:funlen
	numStatJobs, inodesPerStat, maximumAverageStatTime int, yamlPath, queue,
	queuesAvoid, now string, s *client.Scheduler) error {
	walkJobs := make([]*jobqueue.Job, len(desiredPaths))
	combineJobs := make([]*jobqueue.Job, len(desiredPaths))
	tidyJobs := make([]*jobqueue.Job, len(desiredPaths))
	cmd := buildWalkCommand(s, numStatJobs, inodesPerStat, yamlPath, queue, queuesAvoid, maximumAverageStatTime)
	reqWalk, reqCombine := reqs()
	reqWalk.Cores = 3

	var (
		limit     []string
		limitDate int64
	)

	if timeout > 0 {
		maxStart := time.Now().Add(time.Duration(timeout) * time.Hour)
		limitDate = maxStart.Unix()
		limit = []string{"datetime<" + maxStart.Format(time.DateTime)}
	}

	for i, path := range desiredPaths {
		walkUnique := client.UniqueString()
		combineUnique := client.UniqueString()
		finalDirBase := fmt.Sprintf("%s_%s", now, neaten.EncodePath(path))
		outDir := filepath.Join(outputRoot, finalDirBase)
		finalOutput := filepath.Join(finalDirParent, finalDirBase)

		walkJobs[i] = s.NewJob(fmt.Sprintf("%s-d %s -t %d -o %s -i %s %s",
			cmd, walkUnique, limitDate, outDir, statRepGrp(path, unique, now), path),
			walkRepGrp(path, unique, now), "wrstat-walk", walkUnique, "", reqWalk)
		walkJobs[i].LimitGroups = limit

		combineJobs[i] = s.NewJob(fmt.Sprintf("%s combine %q", s.Executable(), outDir),
			combineRepGrp(path, unique, now), "wrstat-combine", combineUnique, walkUnique, reqCombine)
		combineJobs[i].LimitGroups = limit

		tidyJobs[i] = s.NewJob(fmt.Sprintf("%s tidy -f %q %q",
			s.Executable(), finalOutput, outDir),
			tidyRepGrp(path, unique, now), "wrstat-tidy", "", combineUnique, client.DefaultRequirements())
	}

	for _, jobs := range [...][]*jobqueue.Job{walkJobs, combineJobs, tidyJobs} {
		if err := addJobsToQueue(s, jobs); err != nil {
			return err
		}
	}

	return nil
}

// buildWalkCommand builds a wrstat walk command line based on the given n,
// yaml path, queue, and if sudo is in effect.
func buildWalkCommand(s *client.Scheduler, numStatJobs, inodesPerStat int, //nolint:funlen,gocyclo
	yamlPath, queue, queuesAvoid string, maximumAverageStatTime int) string {
	cmd := walkExecutable(s) + " walk "

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

	if recordStats > 0 {
		cmd += fmt.Sprintf("-s %d ", recordStats)
	}

	if sudo {
		cmd += "--sudo "
	}

	if statBlockSize {
		cmd += "-b "
	}

	if maximumAverageStatTime != defaultMaximumAveerageStatTime {
		cmd += "-M " + strconv.Itoa(maximumAverageStatTime)
	}

	return cmd
}

// reqs returns Requirements suitable for walk and combine jobs.
func reqs() (*jqs.Requirements, *jqs.Requirements) {
	req := client.DefaultRequirements()
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
func walkRepGrp(dir, unique, now string) string {
	return repGrp("walk", dir, unique, now)
}

// combineRepGrp returns a rep_grp that can be used for the combine jobs multi
// will create.
func combineRepGrp(dir, unique, now string) string {
	return repGrp("combine", dir, unique, now)
}

// tidyRepGrp returns a rep_grp that can be used for the tidy jobs multi will
// create.
func tidyRepGrp(dir, unique, now string) string {
	return repGrp("tidy", dir, unique, now)
}

func scheduleCleanupJob(s *client.Scheduler, timeout int64, outputRoot,
	jobUnique, logOutput, jobOutput, now string) error {
	cmd := fmt.Sprintf("%s cleanup -w %q -j %q", s.Executable(), outputRoot, jobUnique)

	if logOutput != "" {
		cmd += fmt.Sprintf(" -l %q", logOutput)
	}

	if jobOutput != "" {
		cmd += fmt.Sprintf(" -r -L %q", jobOutput)
	}

	job := s.NewJob(cmd, "wrstat-cleanup-"+now,
		"wrstat-cleanup", "", "", client.DefaultRequirements())
	job.LimitGroups = []string{time.Now().Add(time.Hour*time.Duration(timeout)).Format(time.DateTime) + "<datetime"}

	return addJobsToQueue(s, []*jobqueue.Job{job})
}
