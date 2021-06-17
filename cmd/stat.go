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
	"bufio"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/stat"
)

type Error string

func (e Error) Error() string { return string(e) }

const lstatTimeout = 10 * time.Second
const lstatRetries = 3
const lstatSlowErr = Error("taking longer than 1 second")
const reportFrequency = 10 * time.Second
const nanosecondsInSecond = 1000000000

var statDebug bool

// statCmd represents the stat command.
var statCmd = &cobra.Command{
	Use:   "stat",
	Short: "Stat paths",
	Long: `Stat paths in a given file.

Given a file containing an absolute file path per line (eg. as produced by
'wrstat walk'), this creates a new file with stats for each of those file paths.
The new file is named after the input file with a ".stats" suffix.

The output file format is 11 tab separated columns with the following contents:
1. Base64 encoded path to the file.
2. File size in bytes. If this is greater than the number of bytes in blocks
   allocated, this will be the number of bytes in allocated blocks. (This is to
   account for files with holes in them; as a byproduct, symbolic links will
   be reported as 0 size.)
3. UID.
4. GID.
5. Atime (time of most recent access expressed in seconds).
6. Mtime (time of most recent content modification expressed in seconds.)
7. Ctime (on unix, the time of most recent metadata change in seconds).
8. Filetype:
   'f': regular file
   'l': symbolic link
   's': socket
   'b': block special device file
   'c': character special device file
   'F': FIFO (named pipe)
   'X': anything else
9. Inode number (on unix).
10. Number of hard links.
11. Identifier of the device on which this file resides.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			die("exactly 1 input file should be provided")
		}

		statPathsInFile(args[0], statDebug)
	},
}

func init() {
	RootCmd.AddCommand(statCmd)

	statCmd.Flags().BoolVar(&statDebug, "debug", false, "output Lstat timings")
}

// statPathsInFile does the main work.
func statPathsInFile(inputPath string, debug bool) {
	input, err := os.Open(inputPath)
	if err != nil {
		die("failed to open input file: %s", err)
	}

	defer func() {
		err = input.Close()
		if err != nil {
			warn("failed to close input file: %s", err)
		}
	}()

	scanAndStatInput(input, createStatOutputFile(inputPath), debug)
}

// createStatOutputFile creates a file named input.stats.
func createStatOutputFile(input string) *os.File {
	output, err := os.Create(input + ".stats")
	if err != nil {
		die("failed to create output file: %s", err)
	}

	return output
}

// scanAndStatInput scans through the input, stats each path, and outputs the
// results to the output. If debug is true, outputs timings for Lstat calls.
func scanAndStatInput(input, output *os.File, debug bool) {
	scanner := bufio.NewScanner(input)

	r := &Reporter{Operation: "lstat"}
	if debug {
		r.StartReporting()
		defer r.StopReporting()
	}

	scanLoop(scanner, output, r)

	if err := scanner.Err(); err != nil {
		die("problem reading the input file: %s", err)
	}
}

func scanLoop(scanner *bufio.Scanner, output *os.File, r *Reporter) {
	for scanner.Scan() {
		path := scanner.Text()

		var info fs.FileInfo

		err := r.TimeOperation(func() error {
			var lerr error
			info, lerr = lstat(path, 0)

			return lerr
		})
		if err != nil {
			continue
		}

		_, err = output.WriteString(stat.File(filepath.Dir(path), info).ToString())
		if err != nil {
			die("problem writing to output file: %s", err)
		}
	}
}

// lstat calls os.Lstat() on the given path, but times it out after 1 second and
// retries up to 4 attempts.
func lstat(path string, attempts int) (info fs.FileInfo, err error) {
	infoCh := make(chan fs.FileInfo, 1)
	errCh := make(chan error, 1)

	go func() {
		linfo, lerr := os.Lstat(path)
		infoCh <- linfo
		errCh <- lerr
	}()

	select {
	case err = <-errCh:
		info = <-infoCh

		return
	case <-time.After(lstatTimeout):
		if attempts < lstatRetries {
			warn("an lstat call took longer than 10s, will retry")
			attempts++

			return lstat(path, attempts)
		}

		warn("an lstat call took longer than 10s, giving up")

		err = lstatSlowErr

		return
	}
}

// Reporter can be used to output timing information on how long something is
// taking.
type Reporter struct {
	Operation       string // the name of the operation you will Time(), output in Report()
	currentDuration time.Duration
	totalDuration   time.Duration
	failedDuration  time.Duration
	currentCount    int64
	totalCount      int64
	failedCount     int64
	start           time.Time
	started         bool
	stopCh          chan struct{}
	doneCh          chan struct{}
	sync.Mutex
}

// StartReporting calls Report() regularly.
func (r *Reporter) StartReporting() {
	r.started = true
	r.start = time.Now()
	r.stopCh = make(chan struct{})
	r.doneCh = make(chan struct{})
	ticker := time.NewTicker(reportFrequency)

	go func() {
		for {
			select {
			case <-ticker.C:
				r.Report()
			case <-r.stopCh:
				ticker.Stop()
				r.Report()
				r.ReportFinal()
				close(r.doneCh)

				return
			}
		}
	}()
}

// StopReporting stops the regular calling of Report() and triggers
// ReportFinal().
func (r *Reporter) StopReporting() {
	if !r.started {
		return
	}

	close(r.stopCh)
	<-r.doneCh
}

// Report outputs timings collected since the last Report() call.
func (r *Reporter) Report() {
	r.Lock()
	defer r.Unlock()

	fmt.Printf("%d %s operations in last %s (%s ops/s)\n",
		r.currentCount,
		r.Operation,
		r.currentDuration,
		opsPerSecond(r.currentCount, r.currentDuration))

	r.totalCount += r.currentCount
	r.totalDuration += r.currentDuration
	r.currentCount = 0
	r.currentDuration = 0
}

// ReportFinal reports overall and failed timings.
func (r *Reporter) ReportFinal() {
	fmt.Printf("Overall, %d operations in %s (%s ops/s)\n",
		r.totalCount,
		time.Since(r.start),
		opsPerSecond(r.totalCount, time.Since(r.start)))
	fmt.Printf("Spent %s in actual %s calls (%s ops/s)\n",
		r.totalDuration,
		r.Operation,
		opsPerSecond(r.totalCount, r.totalDuration))

	if r.failedCount > 0 {
		fmt.Printf("There were %d failed %s operations not considered above, which took %s (%s ops/s)\n",
			r.failedCount,
			r.Operation,
			r.failedDuration,
			opsPerSecond(r.failedCount, r.failedDuration))
	}
}

// opsPerSecond returns operations/d.Seconds rounded to 2 decimal places, or n/a
// if either is 0.
func opsPerSecond(ops int64, d time.Duration) string {
	if ops == 0 || d == 0 {
		return "n/a"
	}

	return fmt.Sprintf("%.2f", float64(ops)/float64(d.Nanoseconds())*nanosecondsInSecond)
}

// TimeOperation, if StartReporting() has not yet been called, will simply call
// your given func and return its error. If StartReporting() has been called,
// it will time how long your func takes to run, so that Report() can report
// details about your func.
func (r *Reporter) TimeOperation(f func() error) error {
	if !r.started {
		return f()
	}

	t := time.Now()
	err := f()
	d := time.Since(t)

	r.Lock()
	defer r.Unlock()

	if err != nil {
		r.failedCount++
		r.failedDuration += d
	} else {
		r.currentCount++
		r.currentDuration += d
	}

	return err
}
