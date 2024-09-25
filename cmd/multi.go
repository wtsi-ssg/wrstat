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
	"time"

	"github.com/VertebrateResequencing/wr/jobqueue"
	jqs "github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/v5/merge"
	"github.com/wtsi-ssg/wrstat/v5/scheduler"
)

const (
	walkTime     = 19 * time.Hour
	walkRAM      = 16000
	combineTime  = 40 * time.Minute
	combineRAM   = 800
	mergedbsTime = 15 * time.Minute
	mergedbsRAM  = 42000
	basedirTime  = 15 * time.Minute
	basedirRAM   = 42000
)

// options for this cmd.
var (
	workDir             string
	finalDir            string
	partialDirMerge     string
	partialDirClean     bool
	createPartial       bool
	finishPartial       bool
	multiInodes         int
	multiStatJobs       int
	multiCh             string
	forcedQueue         string
	queuesToAvoid       string
	quota               string
	maxMem              int
	multiBasedirsConfig string
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

A partial walk->stat->combine run can be performed with the --create_partial_dir
flag. These files can be used with the --partial_dir_merge flag to combine this
partial run with a full run. The --partial_dir_clean can be used to provide the
--delete flag to the 'mergedb' subcommand.

Once everything has completed, the final output files are moved to the given
--final_output directory by 'wrstat tidy', with a name that includes the date
this command was started, the basename of the directory operated on, a unique
string per directory of interest, and a unique string for this call of multi:
[year][month][day]_[directory_basename]/[interest unique].[unique].[type]
eg. for 'wrstat multi -i foo -w /path/a -f /path/b /mnt/foo /mnt/bar /home/bar'
It might produce: 
/path/b/20210617_foo.clkdnfnd992nfksj1lld.c35m8359bnc8ni7dgphg.bygroup
/path/b/20210617_foo.clkdnfnd992nfksj1lld.c35m8359bnc8ni7dgphg.byusergroup.gz
/path/b/20210617_foo.clkdnfnd992nfksj1lld.c35m8359bnc8ni7dgphg.logs.gz
/path/b/20210617_foo.clkdnfnd992nfksj1lld.c35m8359bnc8ni7dgphg.stats.gz
/path/b/20210617_bar.f8bns3jkd92kds10k4ks.c35m8359bnc8ni7dgphg.bygroup
/path/b/20210617_bar.f8bns3jkd92kds10k4ks.c35m8359bnc8ni7dgphg.byusergroup.gz
/path/b/20210617_bar.f8bns3jkd92kds10k4ks.c35m8359bnc8ni7dgphg.logs.gz
/path/b/20210617_bar.f8bns3jkd92kds10k4ks.c35m8359bnc8ni7dgphg.stats.gz
/path/b/20210617_bar.d498vhsk39fjh129djg8.c35m8359bnc8ni7dgphg.bygroup
/path/b/20210617_bar.d498vhsk39fjh129djg8.c35m8359bnc8ni7dgphg.byusergroup.gz
/path/b/20210617_bar.d498vhsk39fjh129djg8.c35m8359bnc8ni7dgphg.logs.gz
/path/b/20210617_bar.d498vhsk39fjh129djg8.c35m8359bnc8ni7dgphg.stats.gz
/path/b/20210617.c35m8359bnc8ni7dgphg.basedirs
/path/b/20210617.c35m8359bnc8ni7dgphg.dgut.dbs

The output files will be given the same user:group ownership and
user,group,other read & write permissions as the --final_output directory.

The basedirs.* file gets made by calling 'wrstat basedirs' after the 'combine'
step. The --splits and --mindirs arguments are passed through to basedir;
see docs for the basedir command for values to use.

This requires you provide a --quota file, so that the current max quota
of each group can be recorded. The quota file is a csv of:
gid,disk,size_quota,inode_quota 

This also requires you provide a --owners file, so that the owners of groups can
be recorded. The file format is a csv of gid,owner_name.

Finally, the unique subdirectory of --working_directory that was created is
deleted.

Note that in your --final_output directory, if a *.dgut.dbs directory already
exists, and you have a wrstat server using the database files inside, the server
will automatically start using the new data and delete the old.`,
	Run: func(cmd *cobra.Command, args []string) {
		checkMultiArgs(args)
		err := doMultiScheduling(args, sudo)
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
	multiCmd.Flags().StringVarP(&partialDirMerge, "partial_dir_merge", "l", "", "merge results from a partial run"+
		"stored in the specified directory")
	multiCmd.Flags().BoolVarP(&partialDirClean, "partial_dir_clean", "r", false, "remove old results "+
		"from specified directory after merging")
	multiCmd.Flags().BoolVarP(&createPartial, "create_partial_dir", "p", false, "perform the walk, "+
		"stat, and combine steps only")
	multiCmd.Flags().BoolVarP(&finishPartial, "partial_dir_finish", "z", false, "perform the basedir "+
		"and tidy step on a partial run")
	multiCmd.Flags().IntVarP(&multiInodes, "inodes_per_stat", "n",
		defaultInodesPerJob, "number of inodes per parallel stat job")
	multiCmd.Flags().IntVarP(&multiStatJobs, "num_stat_jobs", "j",
		0, "force a specific number of parallel stat jobs (ignore -n if above 0)")
	multiCmd.Flags().StringVar(&multiCh, "ch", "", "passed through to 'wrstat walk'")
	multiCmd.Flags().StringVar(&forcedQueue, "queue", "", "force a particular queue to be used when scheduling jobs")
	multiCmd.Flags().StringVar(&queuesToAvoid, "queues_avoid", "",
		"force queues with this substring to be avoided when scheduling jobs")
	multiCmd.Flags().StringVarP(&quota, "quota", "q", "", "csv of gid,disk,size_quota,inode_quota")
	multiCmd.Flags().StringVarP(&ownersPath, "owners", "o", "", "gid,owner csv file")
	multiCmd.Flags().IntVarP(&maxMem, "max_mem", "m", basedirRAM, "maximum MBs to reserve for any job")
	multiCmd.Flags().StringVarP(&multiBasedirsConfig, "config", "b", "", "path to basedirs config file")
}

// checkMultiArgs ensures we have the required args for the multi sub-command.
func checkMultiArgs(args []string) {
	if workDir == "" {
		die("--working_directory is required")
	}

	if !createPartial {
		checkStandardFlags()
	}

	if len(args) == 0 {
		die("at least 1 directory of interest must be supplied")
	}
}

func checkStandardFlags() {
	if finalDir == "" {
		die("--final_output is required")
	}

	if quota == "" {
		die("--quota is required")
	}

	if ownersPath == "" {
		die("--owners is required")
	}
}

// doMultiScheduling does the main work of the multi sub-command.
func doMultiScheduling(args []string, sudo bool) error {
	s, d := newScheduler(workDir, forcedQueue, queuesToAvoid, sudo) // TODO
	defer d()

	unique := scheduler.UniqueString()
	outputRoot := filepath.Join(workDir, unique)

	err := os.MkdirAll(outputRoot, userGroupPerm)
	if err != nil {
		return err
	}

	if !finishPartial { //nolint:nestif
		scheduleWalkJobs(outputRoot, args, unique, multiStatJobs, multiInodes, multiCh, forcedQueue, s)

		if partialDirMerge != "" {
			unique = scheduleStaticCopy(outputRoot, unique, partialDirMerge, partialDirClean, s)
		}

		if createPartial {
			s.DisableSudo()
			schedulePartialSentinel(outputRoot, unique, s)

			return nil
		}
	}

	scheduleBasedirsJob(outputRoot, unique, s)
	scheduleTidyJob(outputRoot, finalDir, unique, s)

	return nil
}

// scheduleWalkJobs adds a 'wrstat walk' job to wr's queue for each desired
// path. The second scheduler is used to add combine jobs, which need a memory
// override.
func scheduleWalkJobs(outputRoot string, desiredPaths []string, unique string,
	numStatJobs, inodesPerStat int, yamlPath, queue string, s *scheduler.Scheduler,
) {
	walkJobs := make([]*jobqueue.Job, len(desiredPaths))
	combineJobs := make([]*jobqueue.Job, len(desiredPaths))

	cmd := buildWalkCommand(s, numStatJobs, inodesPerStat, yamlPath, queue)

	reqWalk, reqCombine := reqs()

	for i, path := range desiredPaths {
		thisUnique := scheduler.UniqueString()
		outDir := filepath.Join(outputRoot, filepath.Base(path), thisUnique)

		walkJobs[i] = s.NewJob(fmt.Sprintf("%s -d %s -o %s -i %s %s",
			cmd, thisUnique, outDir, statRepGrp(path, unique), path),
			walkRepGrp(path, unique), "wrstat-walk", thisUnique, "", reqWalk)

		combineJobs[i] = s.NewJob(fmt.Sprintf("%s combine %s", s.Executable(), outDir),
			combineRepGrp(path, unique), "wrstat-combine", unique, thisUnique, reqCombine)
	}

	addJobsToQueue(s, walkJobs)
	addJobsToQueue(s, combineJobs)
}

// buildWalkCommand builds a wrstat walk command line based on the given n,
// yaml path, queue, and if sudo is in effect.
func buildWalkCommand(s *scheduler.Scheduler, numStatJobs, inodesPerStat int, yamlPath, queue string) string {
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

	if sudo {
		cmd += "--sudo "
	}

	return cmd
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

// schedulePartialSentinel adds a job to wr's queue that creates a sentinel file
// indicating the completion of a 'partial' run.
func schedulePartialSentinel(outputRoot, unique string, s *scheduler.Scheduler) {
	job := s.NewJob("touch "+filepath.Join(outputRoot, merge.SentinelComplete),
		repGrp("touchSentinel", "", unique), "wrstat-sentinel", unique+".sentinel",
		unique, scheduler.DefaultRequirements())

	addJobsToQueue(s, []*jobqueue.Job{job})
}

// scheduleBasedirsJob adds a job to wr's queue that creates a base.dirs file
// from the combined dgut.dbs folders.
func scheduleBasedirsJob(outputRoot, unique string, s *scheduler.Scheduler) {
	var baseDirsConfig string

	if multiBasedirsConfig != "" {
		baseDirsConfig = fmt.Sprintf("-b %q", multiBasedirsConfig)
	}

	job := s.NewJob(fmt.Sprintf("%s basedir -q %q -o %q %s %q %q",
		s.Executable(), quota, ownersPath, baseDirsConfig, outputRoot, finalDir),
		repGrp("basedir", "", unique), "wrstat-basedir", unique+".basedir", unique, basedirReqs())

	addJobsToQueue(s, []*jobqueue.Job{job})
}

func copyReqs() *jqs.Requirements {
	req := scheduler.DefaultRequirements()
	req.Time = mergedbsTime
	req.RAM = min(mergedbsRAM, maxMem)

	return req
}

func scheduleStaticCopy(outputRoot, unique, partialDirMerge string, partialDirClean bool,
	s *scheduler.Scheduler,
) string {
	var remove string

	if partialDirClean {
		remove = "--delete"
	}

	thisUnique := unique + ".merge"

	job := s.NewJob(fmt.Sprintf("%s mergedbs %s %q %q",
		s.Executable(), remove, partialDirMerge, outputRoot),
		repGrp("mergedirs", partialDirMerge, unique), "wrstat-merge",
		thisUnique, unique, copyReqs())

	addJobsToQueue(s, []*jobqueue.Job{job})

	return thisUnique
}

// basedirReqs returns Requirements suitable for basedir jobs. The RAM
// requirement is currently set so high due to a bad LSF&cgroups interaction
// that means LSF counts the mmap of the database files as the job's memory
// usage.
func basedirReqs() *jqs.Requirements {
	req := scheduler.DefaultRequirements()
	req.Time = basedirTime
	req.RAM = min(basedirRAM, maxMem)

	return req
}

// scheduleTidyJob adds a job to wr's queue that for each working directory
// subdir moves the output to the final location and then deletes the working
// directory.
func scheduleTidyJob(outputRoot, finalDir, unique string, s *scheduler.Scheduler) {
	job := s.NewJob(fmt.Sprintf("%s tidy -f %s -d %s %s", s.Executable(), finalDir, dateStamp(), outputRoot),
		repGrp("tidy", finalDir, unique), "wrstat-tidy", "", unique+".basedir", scheduler.DefaultRequirements())

	addJobsToQueue(s, []*jobqueue.Job{job})
}
