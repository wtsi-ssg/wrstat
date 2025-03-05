/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
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
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/spf13/cobra"
	ifs "github.com/wtsi-ssg/wrstat/v6/internal/fs"
	"github.com/wtsi-ssg/wrstat/v6/neaten"
)

const uniqueLen = 20

// options for this cmd.
var cleanupDir string
var logsDir string
var jobSuffix string
var removeJob bool
var logJobs string
var cleanupPerms bool

// cleanupCmd represents the cleanup command.
var cleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "Cleanup up aborted multi working files.",
	Long: `Cleanup up aborted multi working files.

If you use 'wrstat multi' but the run fails, you will be left with data in the
--working_directory. If you ran with --sudo, you might not have permission to
delete the data yourself.

By providing the same --working_directory to this command and using sudo, you
can delete the data easily.

Alternatively, to debug an issue you can provide the --perms flag to make all
the sub directories and their files match the perms of the working directory,
instead of deleting them.

To preserve the logs before deletion, you can provide a directory via the
--logs flag to have all files in the working directory that have the '.log'
suffix moved there before deletion.

You can log the current state of the jobs of a 'wrstat multi' run by providing
the --jobs flag with the unique string that is appended to the rep-group. The
--logjobs flag provides the output file for the JSON encoded data.

In addition, you can provide the --remove flag to have the jobs removed from
'wr'.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if cleanupDir == "" {
			die("--working_directory is required")
		}

		if err := run(); err != nil {
			die("%s", err)
		}
	},
}

func run() error { //nolint:gocognit,gocyclo
	var wg sync.WaitGroup

	defer wg.Wait()

	if jobSuffix != "" {
		removeOrLogJobs(jobSuffix, logJobs, removeJob)
	}

	if cleanupPerms {
		if err := matchPerms(cleanupDir); err != nil {
			return fmt.Errorf("could not correct permissions: %w", err)
		}
	}

	if logsDir != "" {
		if err := moveLogs(&wg, cleanupDir, logsDir); err != nil {
			return fmt.Errorf("failed to move logs: %w", err)
		}
	}

	if !cleanupPerms {
		if err := os.RemoveAll(cleanupDir); err != nil {
			return fmt.Errorf("could not cleanup dir: %w", err)
		}
	}

	return nil
}

func removeOrLogJobs(jobSuffix, logJobs string, removeJob bool) {
	s, d := newScheduler("", "", "", false)
	defer d()

	if err := neaten.RemoveOrLogJobs(s, jobSuffix, logJobs, removeJob); err != nil {
		warn("%s", err)
	}
}

func moveLogs(wg *sync.WaitGroup, cleanupDir, logsDir string) error {
	if err := filepath.WalkDir(filepath.Clean(cleanupDir), func(path string, info fs.DirEntry, _ error) error {
		if !info.IsDir() && strings.HasSuffix(path, ".log") {
			if err := moveLog(wg, logsDir, cleanupDir, path); err != nil {
				return err
			}
		}

		return nil
	}); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("could not walk output directory: %w", err)
	}

	return nil
}

func moveLog(wg *sync.WaitGroup, logDir, outputDir, log string) error {
	output := filepath.Join(logDir, strings.TrimPrefix(log, outputDir))

	if err := os.MkdirAll(filepath.Dir(output), ifs.DirPerms); err != nil {
		return err
	}

	if err := os.Rename(log, output); err == nil {
		return nil
	}

	f, err := os.Open(log)
	if err != nil {
		return err
	}

	w, err := os.Create(output)
	if err != nil {
		return err
	}

	wg.Add(1)

	go copyLog(wg, f, w)

	return nil
}

func copyLog(wg *sync.WaitGroup, r io.ReadCloser, w io.WriteCloser) {
	defer wg.Done()
	defer r.Close()
	defer w.Close()

	if _, err := io.Copy(w, r); err != nil {
		warn("error copying log: %s", err)
	}
}

func init() {
	RootCmd.AddCommand(cleanupCmd)

	// flags specific to this sub-command
	cleanupCmd.Flags().StringVarP(&cleanupDir, "working_directory", "w", "",
		"base directory supplied to multi for intermediate results")
	cleanupCmd.Flags().BoolVarP(&cleanupPerms, "perms", "p", false,
		"instead of deleting them, make working subdirectory permissions match the working directory")
	cleanupCmd.Flags().StringVarP(&logsDir, "logs", "l", "", "directory to move logs to before removal")
	cleanupCmd.Flags().StringVarP(&jobSuffix, "jobs", "j", "", "suffix of 'wr multi' jobs to be stopped and/or logged")
	cleanupCmd.Flags().BoolVarP(&removeJob, "remove", "r", false, "remove jobs with the suffix determined by --jobs")
	cleanupCmd.Flags().StringVarP(&logJobs, "logjobs", "L", "", "log info of "+
		"jobs, determined by --jobs, to this output file")
}

func matchPerms(workDir string) error {
	subDirs, err := getWorkingSubDirs(workDir)
	if err != nil {
		return err
	}

	desired, err := os.Stat(workDir)
	if err != nil {
		return err
	}

	for _, path := range subDirs {
		err = filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			return neaten.CorrectPerms(path, desired)
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func getWorkingSubDirs(workDir string) ([]string, error) {
	entries, err := os.ReadDir(workDir)
	if err != nil {
		return nil, err
	}

	var paths []string //nolint:prealloc

	for _, e := range entries {
		if !e.IsDir() || len(e.Name()) != uniqueLen {
			continue
		}

		path := filepath.Join(workDir, e.Name())
		paths = append(paths, path)
	}

	return paths, nil
}
