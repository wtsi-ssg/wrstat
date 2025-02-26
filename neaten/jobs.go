/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
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

package neaten

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/VertebrateResequencing/wr/client"
	"github.com/VertebrateResequencing/wr/jobqueue"
	ifs "github.com/wtsi-ssg/wrstat/v6/internal/fs"
)

const jobsFile = "jobs"

var repGrpRegexp = regexp.MustCompile(`^wrstat-[^-]+\-(.*)-(\d{8})-(\d{6})-[^-]+$`)

// RemoveOrLogJobs will find WR jobs in the scheduler that have IDs with the
// given suffix. The jobs will be sorted by mount point and their data written
// to a file with the following structure:
//
// $logPath/$date-$mountPoint/jobs
//
// With the remove param set to true, the jobs will be removed from WR.
func RemoveOrLogJobs(s *client.Scheduler, suffix, logPath string, remove bool) error {
	jobs, err := s.FindJobsByRepGroupSuffix("-" + suffix)
	if err != nil {
		return fmt.Errorf("error getting jobs: %w", err)
	}

	if remove {
		if err = s.RemoveJobs(jobs...); err != nil {
			return fmt.Errorf("error removing jobs: %w", err)
		}
	}

	if logPath != "" {
		return logJobs(jobs, logPath)
	}

	return nil
}

func logJobs(jobs []*jobqueue.Job, log string) error {
	for mountpoint, jobs := range splitJobsByMountpoint(jobs) {
		if err := writeJobsToLog(jobs, filepath.Join(log, EncodePath(mountpoint), jobsFile)); err != nil {
			return fmt.Errorf("error logging job data: %w", err)
		}
	}

	return nil
}

func splitJobsByMountpoint(jobs []*jobqueue.Job) map[string][]*jobqueue.Job {
	mpJobs := make(map[string][]*jobqueue.Job)

	for _, job := range jobs {
		ms := repGrpRegexp.FindStringSubmatch(job.RepGroup)
		if len(ms) <= 1 {
			continue
		}

		key := ms[2] + ms[3] + "-" + ms[1]

		mpJobs[key] = append(mpJobs[key], job)
	}

	return mpJobs
}

// EncodePath replaces slashes in a path with a unicode slash variant.
func EncodePath(path string) string {
	return strings.ReplaceAll(path, "/", "／")
}

func writeJobsToLog(jobs []*jobqueue.Job, logFile string) error {
	if len(jobs) == 0 {
		return nil
	}

	if err := os.MkdirAll(filepath.Dir(logFile), ifs.DirPerms); err != nil {
		return err
	}

	f, err := os.Create(logFile)
	if err != nil {
		return err
	}

	if err := json.NewEncoder(f).Encode(jobs); err != nil {
		f.Close()

		return err
	}

	return f.Close()
}
