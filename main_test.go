/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
 *
 * Authors:
 *   Michael Woolnough <mw31@sanger.ac.uk>
 *   Daniel Elia <de7@sanger.ac.uk>
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

package main

import (
	"archive/tar"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/VertebrateResequencing/wr/jobqueue"
	"github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/v4/basedirs"
	"github.com/wtsi-ssg/wrstat/v4/dgut"
	"github.com/wtsi-ssg/wrstat/v4/internal/fixtimes"
	"github.com/wtsi-ssg/wrstat/v4/summary"
)

const app = "wrstat_test"

func buildSelf() func() {
	cmd := exec.Command(
		"go", "build", "-tags", "netgo",
		"-ldflags=-X github.com/wtsi-ssg/wrstat/v4/cmd.runJobs=0 -X github.com/wtsi-ssg/wrstat/v4/cmd.Version=TESTVERSION",
		"-o", app,
	)

	cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		failMainTest(err.Error())

		return nil
	}

	return func() {
		os.Remove(app)
	}
}

func failMainTest(err string) {
	fmt.Println(err) //nolint:forbidigo
}

func TestMain(m *testing.M) {
	d1 := buildSelf()
	if d1 == nil {
		return
	}

	defer os.Exit(m.Run())
	defer d1()
}

func runWRStat(args ...string) (string, string, []*jobqueue.Job, error) {
	var (
		stdout, stderr strings.Builder
		jobs           []*jobqueue.Job
	)

	pr, pw, err := os.Pipe()
	if err != nil {
		return "", "", nil, err
	}

	cmd := exec.Command("./"+app, args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.ExtraFiles = append(cmd.ExtraFiles, pw)

	jd := json.NewDecoder(pr)
	done := make(chan struct{})

	go func() {
		for {
			var j []*jobqueue.Job

			if errr := jd.Decode(&j); errr != nil {
				break
			}

			jobs = append(jobs, j...)
		}

		close(done)
	}()

	err = cmd.Run()

	pw.Close()

	<-done

	return stdout.String(), stderr.String(), jobs, err
}

func TestVersion(t *testing.T) {
	Convey("wrstat prints the correct version", t, func() {
		output, stderr, _, err := runWRStat("version")
		So(err, ShouldBeNil)
		So(strings.TrimSpace(output), ShouldEqual, "TESTVERSION")
		So(stderr, ShouldBeBlank)
	})
}

func TestCron(t *testing.T) {
	Convey("For the cron subcommand", t, func() {
		multiTests(t, "cron", "-c", "* * * * * *")
	})
}

func multiTests(t *testing.T, subcommand ...string) {
	t.Helper()

	walkReqs := &scheduler.Requirements{
		RAM:   16000,
		Time:  19 * time.Hour,
		Cores: 1,
		Disk:  1,
	}

	combineReqs := &scheduler.Requirements{
		RAM:   800,
		Time:  40 * time.Minute,
		Cores: 1,
		Disk:  1,
	}

	touchReqs := &scheduler.Requirements{
		RAM:   100,
		Time:  10 * time.Second,
		Cores: 1,
		Disk:  1,
	}

	baseDirsReqs := &scheduler.Requirements{
		RAM:   42000,
		Time:  15 * time.Minute,
		Cores: 1,
		Disk:  1,
	}

	tidyReqs := &scheduler.Requirements{
		RAM:   100,
		Time:  10 * time.Second,
		Cores: 1,
		Disk:  1,
	}

	date := time.Now().Format("20060102")

	Convey("wrstat gets the stats for a partial run", func() {
		workingDir := t.TempDir()
		_, _, jobs, err := runWRStat(append(subcommand, "-w", workingDir, "-p", "/some/path", "/some-other/path")...)
		So(err, ShouldBeNil)

		So(len(jobs), ShouldEqual, 5)
		So(len(jobs[0].DepGroups), ShouldEqual, 1)
		So(len(jobs[1].DepGroups), ShouldEqual, 1)
		So(len(jobs[0].RepGroup), ShouldBeGreaterThan, 20)

		walk1DepGroup := jobs[0].DepGroups[0]
		walk2DepGroup := jobs[1].DepGroups[0]
		repGroup := jobs[0].RepGroup[len(jobs[0].RepGroup)-20:]

		expectation := []*jobqueue.Job{
			{
				Cmd: fmt.Sprintf(" walk -n 1000000  -d %[1]s -o %[2]s/%[3]s/path/%[1]s -i"+
					" wrstat-stat-path-%[4]s-%[3]s /some/path",
					walk1DepGroup, workingDir, repGroup, date),
				CwdMatters:   true,
				RepGroup:     fmt.Sprintf("wrstat-walk-path-%s-%s", date, repGroup),
				ReqGroup:     "wrstat-walk",
				Requirements: walkReqs,
				Override:     1,
				Retries:      30,
				DepGroups:    []string{walk1DepGroup},
			},
			{
				Cmd: fmt.Sprintf(" walk -n 1000000  -d %[1]s -o %[2]s/%[3]s/path/%[1]s"+
					" -i wrstat-stat-path-%[4]s-%[3]s /some-other/path", walk2DepGroup,
					workingDir, repGroup, date),
				CwdMatters:   true,
				RepGroup:     fmt.Sprintf("wrstat-walk-path-%s-%s", date, repGroup),
				ReqGroup:     "wrstat-walk",
				Requirements: walkReqs,
				Override:     1,
				Retries:      30,
				DepGroups:    []string{walk2DepGroup},
			},
			{
				Cmd:          fmt.Sprintf(" combine %s/%s/path/%s", workingDir, repGroup, walk1DepGroup),
				CwdMatters:   true,
				RepGroup:     fmt.Sprintf("wrstat-combine-path-%s-%s", date, repGroup),
				ReqGroup:     "wrstat-combine",
				Requirements: combineReqs,
				Override:     1,
				Retries:      30,
				DepGroups:    []string{repGroup},
				Dependencies: jobqueue.Dependencies{
					{
						DepGroup: walk1DepGroup,
					},
				},
			},
			{
				Cmd:          fmt.Sprintf(" combine %s/%s/path/%s", workingDir, repGroup, walk2DepGroup),
				CwdMatters:   true,
				RepGroup:     fmt.Sprintf("wrstat-combine-path-%s-%s", date, repGroup),
				ReqGroup:     "wrstat-combine",
				Requirements: combineReqs,
				Override:     1,
				Retries:      30,
				DepGroups:    []string{repGroup},
				Dependencies: jobqueue.Dependencies{
					{
						DepGroup: walk2DepGroup,
					},
				},
			},
			{
				Cmd:          fmt.Sprintf("touch %s/%s/combine.complete", workingDir, repGroup),
				CwdMatters:   true,
				RepGroup:     fmt.Sprintf("wrstat-touchSentinel-%s-%s", date, repGroup),
				ReqGroup:     "wrstat-sentinel",
				Requirements: touchReqs,
				Override:     1,
				Retries:      30,
				DepGroups:    []string{repGroup + ".sentinel"},
				Dependencies: jobqueue.Dependencies{
					{
						DepGroup: repGroup,
					},
				},
			},
		}

		So(jobs, ShouldResemble, expectation)
	})

	Convey("wrstat gets the stats for a normal run", func() {
		workingDir := t.TempDir()
		_, _, jobs, err := runWRStat(append(subcommand, "-w", workingDir, "/some/path", "/some-other/path",
			"-f", "final_output", "-q", "quota_file", "-o", "owners_file")...)
		So(err, ShouldBeNil)

		So(len(jobs), ShouldEqual, 6)
		So(len(jobs[0].DepGroups), ShouldEqual, 1)
		So(len(jobs[1].DepGroups), ShouldEqual, 1)
		So(len(jobs[0].RepGroup), ShouldBeGreaterThan, 20)

		walk1DepGroup := jobs[0].DepGroups[0]
		walk2DepGroup := jobs[1].DepGroups[0]
		repGroup := jobs[0].RepGroup[len(jobs[0].RepGroup)-20:]

		expectation := []*jobqueue.Job{
			{
				Cmd: fmt.Sprintf(" walk -n 1000000  -d %[1]s -o %[2]s/%[3]s/path/%[1]s -i"+
					" wrstat-stat-path-%[4]s-%[3]s /some/path", walk1DepGroup,
					workingDir, repGroup, date),
				CwdMatters:   true,
				RepGroup:     fmt.Sprintf("wrstat-walk-path-%s-%s", date, repGroup),
				ReqGroup:     "wrstat-walk",
				Requirements: walkReqs,
				Override:     1,
				Retries:      30,
				DepGroups:    []string{walk1DepGroup},
			},
			{
				Cmd: fmt.Sprintf(" walk -n 1000000  -d %[1]s -o %[2]s/%[3]s/path/%[1]s -i"+
					" wrstat-stat-path-%[4]s-%[3]s /some-other/path", walk2DepGroup,
					workingDir, repGroup, date),
				CwdMatters:   true,
				RepGroup:     fmt.Sprintf("wrstat-walk-path-%s-%s", date, repGroup),
				ReqGroup:     "wrstat-walk",
				Requirements: walkReqs,
				Override:     1,
				Retries:      30,
				DepGroups:    []string{walk2DepGroup},
			},
			{
				Cmd:          fmt.Sprintf(" combine %s/%s/path/%s", workingDir, repGroup, walk1DepGroup),
				CwdMatters:   true,
				RepGroup:     fmt.Sprintf("wrstat-combine-path-%s-%s", date, repGroup),
				ReqGroup:     "wrstat-combine",
				Requirements: combineReqs,
				Override:     1,
				Retries:      30,
				DepGroups:    []string{repGroup},
				Dependencies: jobqueue.Dependencies{
					{
						DepGroup: walk1DepGroup,
					},
				},
			},
			{
				Cmd:          fmt.Sprintf(" combine %s/%s/path/%s", workingDir, repGroup, walk2DepGroup),
				CwdMatters:   true,
				RepGroup:     fmt.Sprintf("wrstat-combine-path-%s-%s", date, repGroup),
				ReqGroup:     "wrstat-combine",
				Requirements: combineReqs,
				Override:     1,
				Retries:      30,
				DepGroups:    []string{repGroup},
				Dependencies: jobqueue.Dependencies{
					{
						DepGroup: walk2DepGroup,
					},
				},
			},
			{
				Cmd: fmt.Sprintf(" basedir -q \"quota_file\" -o \"owners_file\"  \"%s/%s\" \"final_output\"",
					workingDir, repGroup),
				CwdMatters:   true,
				RepGroup:     fmt.Sprintf("wrstat-basedir-%s-%s", date, repGroup),
				ReqGroup:     "wrstat-basedir",
				Requirements: baseDirsReqs,
				Override:     1,
				Retries:      30,
				DepGroups:    []string{repGroup + ".basedir"},
				Dependencies: jobqueue.Dependencies{
					{
						DepGroup: repGroup,
					},
				},
			},
			{
				Cmd:          fmt.Sprintf(" tidy -f final_output -d %s %s/%s", date, workingDir, repGroup),
				CwdMatters:   true,
				RepGroup:     fmt.Sprintf("wrstat-tidy-final_output-%s-%s", date, repGroup),
				ReqGroup:     "wrstat-tidy",
				Requirements: tidyReqs,
				Override:     1,
				Retries:      30,
				Dependencies: jobqueue.Dependencies{
					{
						DepGroup: repGroup + ".basedir",
					},
				},
			},
		}

		So(jobs, ShouldResemble, expectation)
	})

	Convey("wrstat gets the stats for a normal run with a partial merge", func() {
		workingDir := t.TempDir()
		_, _, jobs, err := runWRStat(append(subcommand, "-l", "/path/to/partial_merge", "-w", workingDir, "/some/path",
			"/some-other/path", "-f", "final_output", "-q", "quota_file", "-o", "owners_file")...)
		So(err, ShouldBeNil)

		So(len(jobs), ShouldEqual, 7)
		So(len(jobs[0].DepGroups), ShouldEqual, 1)
		So(len(jobs[1].DepGroups), ShouldEqual, 1)
		So(len(jobs[0].RepGroup), ShouldBeGreaterThan, 20)

		walk1DepGroup := jobs[0].DepGroups[0]
		walk2DepGroup := jobs[1].DepGroups[0]
		repGroup := jobs[0].RepGroup[len(jobs[0].RepGroup)-20:]

		expectation := []*jobqueue.Job{
			{
				Cmd: fmt.Sprintf(" walk -n 1000000  -d %[1]s -o %[2]s/%[3]s/path/%[1]s"+
					" -i wrstat-stat-path-%[4]s-%[3]s /some/path", walk1DepGroup, workingDir, repGroup, date),
				CwdMatters:   true,
				RepGroup:     fmt.Sprintf("wrstat-walk-path-%s-%s", date, repGroup),
				ReqGroup:     "wrstat-walk",
				Requirements: walkReqs,
				Override:     1,
				Retries:      30,
				DepGroups:    []string{walk1DepGroup},
			},
			{
				Cmd: fmt.Sprintf(" walk -n 1000000  -d %[1]s -o %[2]s/%[3]s/path/%[1]s"+
					" -i wrstat-stat-path-%[4]s-%[3]s /some-other/path", walk2DepGroup, workingDir, repGroup, date),
				CwdMatters:   true,
				RepGroup:     fmt.Sprintf("wrstat-walk-path-%s-%s", date, repGroup),
				ReqGroup:     "wrstat-walk",
				Requirements: walkReqs,
				Override:     1,
				Retries:      30,
				DepGroups:    []string{walk2DepGroup},
			},
			{
				Cmd:          fmt.Sprintf(" combine %s/%s/path/%s", workingDir, repGroup, walk1DepGroup),
				CwdMatters:   true,
				RepGroup:     fmt.Sprintf("wrstat-combine-path-%s-%s", date, repGroup),
				ReqGroup:     "wrstat-combine",
				Requirements: combineReqs,
				Override:     1,
				Retries:      30,
				DepGroups:    []string{repGroup},
				Dependencies: jobqueue.Dependencies{
					{
						DepGroup: walk1DepGroup,
					},
				},
			},
			{
				Cmd:          fmt.Sprintf(" combine %s/%s/path/%s", workingDir, repGroup, walk2DepGroup),
				CwdMatters:   true,
				RepGroup:     fmt.Sprintf("wrstat-combine-path-%s-%s", date, repGroup),
				ReqGroup:     "wrstat-combine",
				Requirements: combineReqs,
				Override:     1,
				Retries:      30,
				DepGroups:    []string{repGroup},
				Dependencies: jobqueue.Dependencies{
					{
						DepGroup: walk2DepGroup,
					},
				},
			},
			{
				Cmd:          fmt.Sprintf(" mergedbs  \"/path/to/partial_merge\" \"%s/%s\"", workingDir, repGroup),
				CwdMatters:   true,
				RepGroup:     fmt.Sprintf("wrstat-mergedirs-partial_merge-%s-%s", date, repGroup),
				ReqGroup:     "wrstat-merge",
				Requirements: baseDirsReqs,
				Override:     1,
				Retries:      30,
				DepGroups:    []string{repGroup + ".merge"},
				Dependencies: jobqueue.Dependencies{
					{
						DepGroup: repGroup,
					},
				},
			},
			{
				Cmd: fmt.Sprintf(" basedir -q \"quota_file\" -o \"owners_file\"  \"%s/%s\" \"final_output\"",
					workingDir, repGroup),
				CwdMatters:   true,
				RepGroup:     fmt.Sprintf("wrstat-basedir-%s-%s.merge", date, repGroup),
				ReqGroup:     "wrstat-basedir",
				Requirements: baseDirsReqs,
				Override:     1,
				Retries:      30,
				DepGroups:    []string{repGroup + ".merge.basedir"},
				Dependencies: jobqueue.Dependencies{
					{
						DepGroup: repGroup + ".merge",
					},
				},
			},
			{
				Cmd:          fmt.Sprintf(" tidy -f final_output -d %s %s/%s", date, workingDir, repGroup),
				CwdMatters:   true,
				RepGroup:     fmt.Sprintf("wrstat-tidy-final_output-%s-%s.merge", date, repGroup),
				ReqGroup:     "wrstat-tidy",
				Requirements: tidyReqs,
				Override:     1,
				Retries:      30,
				Dependencies: jobqueue.Dependencies{
					{
						DepGroup: repGroup + ".merge.basedir",
					},
				},
			},
		}

		So(jobs, ShouldResemble, expectation)
	})
}

func TestMulti(t *testing.T) {
	Convey("For the multi subcommand", t, func() {
		multiTests(t, "multi")
	})
}

func TestWalk(t *testing.T) {
	Convey("wrstat prints the correct output for a directory", t, func() {
		out := t.TempDir()
		tmp := t.TempDir()

		for _, dir := range [...]string{"/a/b/c/d/e", "/a/b/f", "/a/g/h"} {
			err := os.MkdirAll(filepath.Join(tmp, dir), 0755)
			So(err, ShouldBeNil)
		}

		for _, file := range [...]string{"/a/b/c/test.txt", "/a/b/f/test2.csv", "/a/test3"} {
			writeFileString(t, filepath.Join(tmp, file), "")
		}

		depgroup := "test-group"

		_, _, jobs, err := runWRStat("walk", tmp, "-o", out, "-d", depgroup, "-j", "1")
		So(err, ShouldBeNil)

		walk1 := filepath.Join(out, "walk.1")

		jobsExpectation := []*jobqueue.Job{
			{
				Cmd:        " stat " + walk1,
				CwdMatters: true,
				RepGroup:   "wrstat-stat-" + filepath.Base(tmp) + "-" + time.Now().Format("20060102"),
				ReqGroup:   "wrstat-stat",
				Requirements: &scheduler.Requirements{
					RAM:   750,
					Time:  12 * time.Hour,
					Cores: 1,
					Disk:  1,
				},
				Override:  1,
				Retries:   30,
				DepGroups: []string{depgroup},
			},
		}

		removeJobRepGroupSuffixes(jobs)

		So(jobs, ShouldResemble, jobsExpectation)

		compareFileContents(t, walk1, fmt.Sprintf(`%[1]s
%[1]s/a/test3
%[1]s/a
%[1]s/a/g
%[1]s/a/g/h
%[1]s/a/b
%[1]s/a/b/f/test2.csv
%[1]s/a/b/f
%[1]s/a/b/c/test.txt
%[1]s/a/b/c
%[1]s/a/b/c/d
%[1]s/a/b/c/d/e`, tmp))

		_, _, jobs, err = runWRStat("walk", tmp, "-o", out, "-d", depgroup, "-j", "2")
		So(err, ShouldBeNil)

		walk2 := filepath.Join(out, "walk.2")

		jobsExpectation = []*jobqueue.Job{
			{
				Cmd:        " stat " + walk1,
				CwdMatters: true,
				RepGroup:   "wrstat-stat-" + filepath.Base(tmp) + "-" + time.Now().Format("20060102"),
				ReqGroup:   "wrstat-stat",
				Requirements: &scheduler.Requirements{
					RAM:   750,
					Time:  12 * time.Hour,
					Cores: 1,
					Disk:  1,
				},
				Override:  1,
				Retries:   30,
				DepGroups: []string{depgroup},
			},
			{
				Cmd:        " stat " + walk2,
				CwdMatters: true,
				RepGroup:   "wrstat-stat-" + filepath.Base(tmp) + "-" + time.Now().Format("20060102"),
				ReqGroup:   "wrstat-stat",
				Requirements: &scheduler.Requirements{
					RAM:   750,
					Time:  12 * time.Hour,
					Cores: 1,
					Disk:  1,
				},
				Override:  1,
				Retries:   30,
				DepGroups: []string{depgroup},
			},
		}

		removeJobRepGroupSuffixes(jobs)

		So(jobs, ShouldResemble, jobsExpectation)
	})
}

func writeFileString(t *testing.T, path, contents string) {
	t.Helper()

	f, err := os.Create(path)
	So(err, ShouldBeNil)

	_, err = io.WriteString(f, contents)
	So(err, ShouldBeNil)
	So(f.Close(), ShouldBeNil)
}

func compareFileContents(t *testing.T, filename, expectation string) {
	t.Helper()

	f, err := os.Open(filename)
	So(err, ShouldBeNil)

	defer f.Close()

	output, err := io.ReadAll(f)
	So(err, ShouldBeNil)

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	expectedLines := strings.Split(strings.TrimSpace(expectation), "\n")

	sort.Slice(lines, func(i, j int) bool { return lines[i] < lines[j] })
	sort.Slice(expectedLines, func(i, j int) bool { return expectedLines[i] < expectedLines[j] })

	So(lines, ShouldResemble, expectedLines)
}

func removeJobRepGroupSuffixes(jobs []*jobqueue.Job) {
	for _, job := range jobs {
		job.RepGroup = job.RepGroup[:len(job.RepGroup)-21]
	}
}

func TestStat(t *testing.T) {
	type File struct {
		name   string
		length int
		mtime  time.Time
	}

	tmp := t.TempDir()

	Convey("Given a valid walk file, the stats file prints the correct output", t, func() {
		var (
			inodes         []uint64
			dev            uint64
			atimes, ctimes []int64
		)

		for _, stats := range [...]File{
			{
				name:   "aDirectory/aFile",
				mtime:  time.Unix(7383773, 0),
				length: 10,
			},
			{
				name:  "aDirectory/aSubDirectory",
				mtime: time.Unix(314159, 0),
			},
			{
				name:  "aDirectory",
				mtime: time.Unix(133032, 0),
			},
			{
				name:  "anotherDirectory",
				mtime: time.Unix(282820, 0),
			},
			{
				name:  ".",
				mtime: time.Unix(271828, 0),
			},
		} {
			path := filepath.Join(tmp, stats.name)

			if stats.length > 0 {
				err := os.MkdirAll(filepath.Dir(path), 0755)
				So(err, ShouldBeNil)

				writeFileString(t, path, strings.Repeat("\x00", stats.length))
			} else {
				err := os.MkdirAll(path, 0755)
				So(err, ShouldBeNil)
			}

			stat, err := os.Stat(path)
			So(err, ShouldBeNil)

			statt, ok := stat.Sys().(*syscall.Stat_t)
			So(ok, ShouldBeTrue)

			inodes = append(inodes, statt.Ino)
			dev = statt.Dev
			atimes = append(atimes, statt.Atim.Sec)
			ctimes = append(ctimes, statt.Ctim.Sec)

			err = os.Chtimes(path, time.Time{}, stats.mtime)
			So(err, ShouldBeNil)
		}

		statDir := t.TempDir()
		statFilePath := filepath.Join(statDir, "dir.walk")
		statFile, err := os.Create(statFilePath)
		So(err, ShouldBeNil)

		err = fs.WalkDir(os.DirFS(tmp), ".", func(path string, _ fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			_, err = io.WriteString(statFile, filepath.Join(tmp, path)+"\n")
			So(err, ShouldBeNil)

			return nil
		})
		So(err, ShouldBeNil)

		err = statFile.Close()
		So(err, ShouldBeNil)

		_, _, jobs, err := runWRStat("stat", statFilePath)

		So(err, ShouldBeNil)
		So(len(jobs), ShouldEqual, 0)

		u, err := user.Current()
		So(err, ShouldBeNil)

		g, err := user.LookupGroupId(u.Gid)
		So(err, ShouldBeNil)

		statsExpectation := fmt.Sprintf(""+
			"%[3]s\t4096\t%[1]s\t%[2]s\t%[14]d\t271828\t%[19]d\td\t%[8]d\t4\t%[13]d\n"+
			"%[4]s\t4096\t%[1]s\t%[2]s\t%[15]d\t133032\t%[20]d\td\t%[9]d\t3\t%[13]d\n"+
			"%[5]s\t10\t%[1]s\t%[2]s\t%[16]d\t7383773\t%[21]d\tf\t%[10]d\t1\t%[13]d\n"+
			"%[6]s\t4096\t%[1]s\t%[2]s\t%[17]d\t314159\t%[22]d\td\t%[11]d\t2\t%[13]d\n"+
			"%[7]s\t4096\t%[1]s\t%[2]s\t%[18]d\t282820\t%[23]d\td\t%[12]d\t2\t%[13]d\n",
			u.Uid,
			u.Gid,
			base64.StdEncoding.EncodeToString([]byte(tmp)),
			base64.StdEncoding.EncodeToString([]byte(filepath.Join(tmp, "aDirectory"))),
			base64.StdEncoding.EncodeToString([]byte(filepath.Join(tmp, "aDirectory", "aFile"))),
			base64.StdEncoding.EncodeToString([]byte(filepath.Join(tmp, "aDirectory", "aSubDirectory"))),
			base64.StdEncoding.EncodeToString([]byte(filepath.Join(tmp, "anotherDirectory"))),
			inodes[4],
			inodes[2],
			inodes[0],
			inodes[1],
			inodes[3],
			dev,
			atimes[4],
			atimes[3],
			atimes[2],
			atimes[0],
			atimes[1],
			ctimes[4],
			ctimes[3],
			ctimes[2],
			ctimes[0],
			ctimes[1],
		)

		fi, err := os.Lstat(tmp)
		So(err, ShouldBeNil)

		atime := fi.Sys().(*syscall.Stat_t).Atim.Sec //nolint:forcetypeassert

		groupExpectation := fmt.Sprintf("%s\t%s\t1\t10\n", g.Name, u.Username)

		parent := tmp

		var userGroupExpectation, walkExpectations string

		for filepath.Dir(parent) != parent {
			parent = filepath.Dir(parent)

			userGroupExpectation = fmt.Sprintf("%s\t%s\t%s\t1\t10\n", u.Username, g.Name, parent) + userGroupExpectation
			walkExpectations = fmt.Sprintf(""+
				"%[1]s\t%[2]s\t%[3]s\t0\t1\t10\t%[4]d\t7383773\n"+
				"%[1]s\t%[2]s\t%[3]s\t1\t5\t16394\t%[4]d\t7383773\n"+
				"%[1]s\t%[2]s\t%[3]s\t15\t4\t16384\t%[4]d\t314159\n", parent, g.Gid, u.Uid, atime) + walkExpectations
		}

		userGroupExpectation += fmt.Sprintf(""+
			"%[1]s\t%[2]s\t%[3]s\t1\t10\n"+
			"%[1]s\t%[2]s\t%[3]s/aDirectory\t1\t10\n", u.Username, g.Name, tmp)

		walkExpectations += fmt.Sprintf(""+
			"%[1]s\t%[2]s\t%[3]s\t0\t1\t10\t%[4]d\t7383773\n"+
			"%[1]s\t%[2]s\t%[3]s\t1\t5\t16394\t%[4]d\t7383773\n"+
			"%[1]s\t%[2]s\t%[3]s\t15\t4\t16384\t%[4]d\t314159\n"+
			"%[1]s/aDirectory\t%[2]s\t%[3]s\t0\t1\t10\t%[4]d\t7383773\n"+
			"%[1]s/aDirectory\t%[2]s\t%[3]s\t1\t3\t8202\t%[4]d\t7383773\n"+
			"%[1]s/aDirectory\t%[2]s\t%[3]s\t15\t2\t8192\t%[4]d\t314159\n"+
			"%[1]s/aDirectory/aSubDirectory\t%[2]s\t%[3]s\t1\t1\t4096\t%[4]d\t314159\n"+
			"%[1]s/aDirectory/aSubDirectory\t%[2]s\t%[3]s\t15\t1\t4096\t%[4]d\t314159\n"+
			"%[1]s/anotherDirectory\t%[2]s\t%[3]s\t1\t1\t4096\t%[4]d\t282820\n"+
			"%[1]s/anotherDirectory\t%[2]s\t%[3]s\t15\t1\t4096\t%[4]d\t282820\n", tmp, g.Gid, u.Uid, atime)

		for file, contents := range map[string]string{
			"dir.walk.stats":       statsExpectation,
			"dir.walk.bygroup":     groupExpectation,
			"dir.walk.byusergroup": userGroupExpectation, "dir.walk.dgut": walkExpectations,
			"dir.walk.log": "",
		} {
			f, err := os.Open(filepath.Join(statDir, file))
			So(err, ShouldBeNil)

			data, err := io.ReadAll(f)
			f.Close()
			So(err, ShouldBeNil)
			So(string(data), ShouldEqual, contents)
		}
	})
}

func TestCombine(t *testing.T) {
	Convey("For the combine subcommand, it creates the expected output", t, func() {
		tmp := t.TempDir()

		for file, contents := range map[string]string{
			"a.stats":       "a\nb\nc\n",
			"b.stats":       "d\ne\nf\ng\n",
			"c.stats":       "h\n",
			"a.byusergroup": "c\t1\t2\nb\t3\t4\na\t5\t6\n",
			"b.byusergroup": "a\t7\t8\nd\t9\t10\nc\t11\t12\nb\t13\t14\n",
			"c.byusergroup": "f\t15\t16\ne\t17\t18\nb\t19\t20\n",
			"a.bygroup":     "a\tb\tc\td\n1\t2\t3\t4\n",
			"b.bygroup":     "e\tf\tg\th\n5\t6\t7\t8\n",
			"c.bygroup":     "",
			"a.dgut": "" +
				"/\t2000\t1000\t0\t1\t10\t1721915848\t7383773\n" +
				"/\t2000\t1000\t2\t5\t16394\t1721915848\t7383773\n" +
				"/\t2000\t1000\t15\t4\t16384\t1721915848\t314159\n" +
				"/some\t2000\t1000\t0\t1\t10\t1721915848\t7383773\n" +
				"/some\t2000\t1000\t2\t5\t16394\t1721915848\t7383773\n" +
				"/some\t2000\t1000\t15\t4\t16384\t1721915848\t314159\n" +
				"/some/directory\t2000\t1000\t0\t1\t10\t1721915848\t7383773\n" +
				"/some/directory\t2000\t1000\t2\t5\t16394\t1721915848\t7383773\n" +
				"/some/directory\t2000\t1000\t15\t4\t16384\t1721915848\t314159\n" +
				"/some/directory/001\t2000\t1000\t0\t1\t10\t1721915848\t7383773\n" +
				"/some/directory/001\t2000\t1000\t2\t5\t16394\t1721915848\t7383773\n" +
				"/some/directory/001\t2000\t1000\t15\t4\t16384\t1721915848\t314159\n" +
				"/some/directory/001/aDirectory\t2000\t1000\t0\t1\t10\t1721915848\t7383773\n" +
				"/some/directory/001/aDirectory\t2000\t1000\t2\t3\t8202\t1721915848\t7383773\n" +
				"/some/directory/001/aDirectory\t2000\t1000\t15\t2\t8192\t1721915848\t314159\n" +
				"/some/directory/001/aDirectory/aSubDirectory\t2000\t1000\t2\t1\t4096\t1721915848\t314159\n" +
				"/some/directory/001/aDirectory/aSubDirectory\t2000\t1000\t15\t1\t4096\t1721915848\t314159\n" +
				"/some/directory/001/anotherDirectory\t2000\t1000\t2\t1\t4096\t1721915848\t282820\n" +
				"/some/directory/001/anotherDirectory\t2000\t1000\t15\t1\t4096\t1721915848\t282820\n",
			"a.log": "A log file\nwith 2 lines\n",
			"b.log": "Another log file, with 1 line\n",
			"c.log": "Lorem ipsum!!!!",
		} {
			writeFileString(t, filepath.Join(tmp, file), contents)
		}

		_, _, jobs, err := runWRStat("combine", tmp)
		So(err, ShouldBeNil)
		So(len(jobs), ShouldEqual, 0)

		for file, contents := range map[string]string{
			"combine.stats.gz": "a\nb\nc\nd\ne\nf\ng\nh\n",
			"combine.byusergroup.gz": "a\t7\t8\nc\t1\t2\nb\t3\t4\na\t5\t6\nd\t9\t10\n" +
				"c\t11\t12\nb\t13\t14\nf\t15\t16\ne\t17\t18\nb\t19\t20\n",
			"combine.bygroup": "a\tb\tc\td\n1\t2\t3\t4\ne\tf\tg\th\n5\t6\t7\t8\n",
			"combine.log.gz":  "A log file\nwith 2 lines\nAnother log file, with 1 line\nLorem ipsum!!!!",
		} {
			f, errr := os.Open(filepath.Join(tmp, file))
			So(errr, ShouldBeNil)

			var r io.Reader

			if strings.HasSuffix(file, ".gz") {
				r, err = gzip.NewReader(f)
				So(err, ShouldBeNil)
			} else {
				r = f
			}

			buf, errr := io.ReadAll(r)
			So(errr, ShouldBeNil)

			f.Close()

			So(string(buf), ShouldEqual, contents)
		}

		db := dgut.NewDB(filepath.Join(tmp, "combine.dgut.db"))

		err = db.Open()
		So(err, ShouldBeNil)

		info, err := db.Info()
		So(err, ShouldBeNil)
		So(info.NumDirs, ShouldEqual, 7)
		So(info.NumDGUTs, ShouldEqual, 19)
		So(info.NumParents, ShouldEqual, 5)
		So(info.NumChildren, ShouldEqual, 6)

		uids := []uint32{1000}
		gids := []uint32{2000}

		for _, test := range [...]struct {
			Directory   string
			Filter      *dgut.Filter
			NumFiles    uint64
			TotalSize   uint64
			NewestMTime int64
			UIDs        []uint32
			GIDs        []uint32
			FTs         []summary.DirGUTFileType
		}{
			{
				Directory:   "/",
				NumFiles:    10,
				TotalSize:   32788,
				NewestMTime: 7383773,
				UIDs:        uids,
				GIDs:        gids,
				FTs:         []summary.DirGUTFileType{0, 2, 15},
			},
			{
				Directory:   "/some/directory/001",
				NumFiles:    10,
				TotalSize:   32788,
				NewestMTime: 7383773,
				UIDs:        uids,
				GIDs:        gids,
				FTs:         []summary.DirGUTFileType{0, 2, 15},
			},
			{
				Directory:   "/some/directory/001",
				Filter:      &dgut.Filter{FTs: []summary.DirGUTFileType{0, 2, 15}},
				NumFiles:    10,
				TotalSize:   32788,
				NewestMTime: 7383773,
				UIDs:        uids,
				GIDs:        gids,
				FTs:         []summary.DirGUTFileType{0, 2, 15},
			},
			{
				Directory:   "/some/directory/001",
				Filter:      &dgut.Filter{FTs: []summary.DirGUTFileType{0}},
				NumFiles:    1,
				TotalSize:   10,
				NewestMTime: 7383773,
				UIDs:        uids,
				GIDs:        gids,
				FTs:         []summary.DirGUTFileType{0},
			},
			{
				Directory:   "/some/directory/001/aDirectory",
				NumFiles:    6,
				TotalSize:   16404,
				NewestMTime: 7383773,
				UIDs:        uids,
				GIDs:        gids,
				FTs:         []summary.DirGUTFileType{0, 2, 15},
			},
			{
				Directory:   "/some/directory/001/aDirectory/aSubDirectory",
				NumFiles:    2,
				TotalSize:   8192,
				NewestMTime: 314159,
				UIDs:        uids,
				GIDs:        gids,
				FTs:         []summary.DirGUTFileType{2, 15},
			},
			{
				Directory: "/some/directory/001/aDirectory/aSubDirectory",
				Filter:    &dgut.Filter{UIDs: []uint32{0}},
				UIDs:      []uint32{},
				GIDs:      []uint32{},
				FTs:       []summary.DirGUTFileType{},
			},
		} {
			numFiles, totalSize, _, newestMTime, uids, gids, fts, _, err := db.DirInfo(test.Directory, test.Filter)
			So(err, ShouldBeNil)
			So(numFiles, ShouldEqual, test.NumFiles)
			So(totalSize, ShouldEqual, test.TotalSize)
			So(newestMTime, ShouldEqual, test.NewestMTime)
			So(uids, ShouldResemble, test.UIDs)
			So(gids, ShouldResemble, test.GIDs)
			So(fts, ShouldResemble, test.FTs)
		}
	})
}

func TestMergsDBs(t *testing.T) {
	Convey("For the mergedbs subcommand, it copies and delete the correct files", t, func() {
		srcDir := t.TempDir()
		destDir := t.TempDir()

		for dir, mtime := range map[string]time.Time{
			"a": time.Unix(1, 0),
			"b": time.Unix(3, 0),
			"c": time.Unix(2, 0),
		} {
			fullDir := filepath.Join(srcDir, dir)

			for file, mt := range map[string]time.Time{
				"test":             mtime.Add(1 * time.Second),
				"combine.complete": mtime.Add(2 * time.Second),
				"subdir/file":      mtime.Add(3 * time.Second),
			} {
				p := filepath.Join(fullDir, file)

				err := os.MkdirAll(filepath.Dir(p), 0755)
				So(err, ShouldBeNil)

				writeFileString(t, p, filepath.Join(dir, file))

				err = os.Chtimes(p, mt, mt)
				So(err, ShouldBeNil)
			}

			err := os.Chtimes(fullDir, mtime, mtime)
			So(err, ShouldBeNil)
		}

		newerDir := filepath.Join(srcDir, "d")

		err := os.MkdirAll(newerDir, 0755)
		So(err, ShouldBeNil)

		err = os.Chtimes(newerDir, time.Unix(4, 0), time.Unix(4, 0))
		So(err, ShouldBeNil)

		_, _, jobs, err := runWRStat("mergedbs", "-d", srcDir, destDir)
		So(err, ShouldBeNil)
		So(len(jobs), ShouldEqual, 0)

		entries, err := fs.ReadDir(os.DirFS(srcDir), ".")
		So(err, ShouldBeNil)
		So(len(entries), ShouldResemble, 2)
		So(entries[0].Name(), ShouldEqual, "b")
		So(entries[1].Name(), ShouldEqual, "d")

		for file, mt := range map[string]time.Time{
			"test":             time.Unix(4, 0),
			"combine.complete": time.Unix(5, 0),
			"subdir/file":      time.Unix(6, 0),
		} {
			p := filepath.Join(destDir, file)

			fi, err := os.Lstat(p)
			So(err, ShouldBeNil)
			So(fi.ModTime(), ShouldEqual, mt)

			f, err := os.Open(p)
			So(err, ShouldBeNil)

			contents, err := io.ReadAll(f)
			So(err, ShouldBeNil)
			So(string(contents), ShouldEqual, filepath.Join("b", file))

			f.Close()
		}
	})
}

type stringCloser struct {
	io.WriteCloser
}

func (sc *stringCloser) WriteString(s string) (int, error) {
	return io.WriteString(sc.WriteCloser, s)
}

type fileInfo struct {
	filename            string
	isDir               bool
	uid, gid            uint32
	atime, ctime, mtime int64
	size                int64
}

func (f fileInfo) Name() string { return f.filename }

func (fileInfo) Mode() fs.FileMode { return 0 }

func (fileInfo) ModTime() time.Time { return time.Time{} }

func (f fileInfo) IsDir() bool { return f.isDir }

func (f fileInfo) Size() int64 { return f.size }

func (f fileInfo) Sys() any {
	return &syscall.Stat_t{
		Uid:  f.uid,
		Gid:  f.gid,
		Atim: syscall.Timespec{Sec: f.atime},
		Ctim: syscall.Timespec{Sec: f.ctime},
		Mtim: syscall.Timespec{Sec: f.mtime},
	}
}

func TestBasedirs(t *testing.T) {
	Convey("For the basedir subcommand, it creates the expected output", t, func() {
		configs := t.TempDir()
		dbTmp := t.TempDir()
		outputTmp := t.TempDir()

		for file, contents := range map[string]string{
			"owners": "" +
				"9000,BOM1\n" +
				"9001,BOM2",
			"quota": "",
			"baseDirsConfig": "" +
				"/someDirectory/\t2\t2\n" +
				"/someDirectory/a/mdt0\t3\t3",
		} {
			writeFileString(t, filepath.Join(configs, file), contents)
		}

		err := os.MkdirAll(filepath.Join(dbTmp, "a", "b", "combine.dgut.db"), 0755)
		So(err, ShouldBeNil)

		db := dgut.NewDB(filepath.Join(dbTmp, "a", "b", "combine.dgut.db"))

		pr, pw := io.Pipe()

		o := summary.NewByDirGroupUserType()

		const dirSize = 4096

		for _, info := range [...]fs.FileInfo{
			fileInfo{
				filename: "/someDirectory",
				atime:    1,
				ctime:    2,
				mtime:    3,
				size:     dirSize,
				isDir:    true,
			},
			fileInfo{
				filename: "/someDirectory/a",
				atime:    4,
				ctime:    5,
				mtime:    6,
				size:     dirSize,
				isDir:    true,
			},
			fileInfo{
				filename: "/someDirectory/b",
				uid:      8010,
				gid:      9001,
				atime:    7,
				ctime:    8,
				mtime:    9,
				size:     dirSize,
				isDir:    true,
			},
			fileInfo{
				filename: "/someDirectory/c",
				atime:    10,
				ctime:    11,
				mtime:    12,
				size:     dirSize,
				isDir:    true,
			},
			fileInfo{
				filename: "/someDirectory/a/mdt0",
				atime:    13,
				ctime:    14,
				mtime:    15,
				size:     dirSize,
				isDir:    true,
			},
			fileInfo{
				filename: "/someDirectory/a/team1",
				atime:    16,
				ctime:    17,
				mtime:    18,
				size:     dirSize,
				isDir:    true,
			},
			fileInfo{
				filename: "/someDirectory/a/team1/x.vcf",
				uid:      8000,
				gid:      9000,
				atime:    19,
				ctime:    20,
				mtime:    21,
				size:     10,
			},
			fileInfo{
				filename: "/someDirectory/a/team1/v.vcf",
				uid:      8003,
				gid:      9000,
				atime:    22,
				ctime:    23,
				mtime:    24,
				size:     10,
			},
			fileInfo{
				filename: "/someDirectory/a/team1/y.sam",
				uid:      8001,
				gid:      9000,
				atime:    25,
				ctime:    26,
				mtime:    27,
				size:     50,
			},
			fileInfo{
				filename: "/someDirectory/a/team1/z.txt",
				uid:      8002,
				gid:      9000,
				atime:    28,
				ctime:    29,
				mtime:    30,
				size:     100,
			},
			fileInfo{
				filename: "/someDirectory/a/team1/z.txt.old",
				uid:      8002,
				gid:      9000,
				atime:    31,
				ctime:    32,
				mtime:    33,
				size:     90,
			},
			fileInfo{
				filename: "/someDirectory/a/team1/folder",
				uid:      8000,
				gid:      9000,
				atime:    34,
				ctime:    35,
				mtime:    36,
				size:     dirSize,
				isDir:    true,
			},
			fileInfo{
				filename: "/someDirectory/a/mdt0/team2",
				uid:      8010,
				gid:      9001,
				atime:    37,
				ctime:    38,
				mtime:    39,
				size:     dirSize,
				isDir:    true,
			},
			fileInfo{
				filename: "/someDirectory/a/mdt0/team2/helloworld.py",
				uid:      8010,
				gid:      9001,
				atime:    40,
				ctime:    41,
				mtime:    42,
				size:     8092,
			},
			fileInfo{
				filename: "/someDirectory/a/mdt0/team2/README.md",
				uid:      8010,
				gid:      9001,
				atime:    43,
				ctime:    44,
				mtime:    45,
				size:     5000,
			},
			fileInfo{
				filename: "/someDirectory/a/mdt0/team2/data.ped",
				uid:      8010,
				gid:      9001,
				atime:    46,
				ctime:    47,
				mtime:    48,
				size:     6000,
			},
			fileInfo{
				filename: "/someDirectory/a/team2",
				uid:      8010,
				gid:      9001,
				atime:    49,
				ctime:    50,
				mtime:    51,
				size:     dirSize,
				isDir:    true,
			},
			fileInfo{
				filename: "/someDirectory/b/team2/test.log",
				uid:      8010,
				gid:      9001,
				atime:    52,
				ctime:    53,
				mtime:    54,
				size:     100,
			},
			fileInfo{
				filename: "/someDirectory/b/team2/test.vcf",
				uid:      8010,
				gid:      9001,
				atime:    55,
				ctime:    56,
				mtime:    57,
				size:     100,
			},
			fileInfo{
				filename: "/someDirectory/c/team3",
				uid:      8000,
				gid:      9000,
				atime:    58,
				ctime:    59,
				mtime:    60,
				size:     dirSize,
				isDir:    true,
			},
			fileInfo{
				filename: "/someDirectory/c/team3/this.txt",
				uid:      8000,
				gid:      9000,
				atime:    61,
				ctime:    62,
				mtime:    63,
				size:     10,
			},
			fileInfo{
				filename: "/someDirectory/c/team3/that.txt",
				uid:      8000,
				gid:      9000,
				atime:    64,
				ctime:    65,
				mtime:    66,
				size:     20,
			},
		} {
			err = o.Add(info.Name(), info)
			So(err, ShouldBeNil)
		}

		go o.Output(&stringCloser{WriteCloser: pw}) //nolint:errcheck

		err = db.Store(pr, 10000)
		So(err, ShouldBeNil)

		db.Close()

		_, _, jobs, err := runWRStat("basedir", "-q", filepath.Join(configs, "quota"), "-o",
			filepath.Join(configs, "owners"), "-b", filepath.Join(configs, "baseDirsConfig"), dbTmp, outputTmp)
		So(err, ShouldBeNil)
		So(len(jobs), ShouldEqual, 0)

		bdr, err := basedirs.NewReader(filepath.Join(dbTmp, "basedirs.db"), filepath.Join(configs, "owners"))
		So(err, ShouldBeNil)

		gu, err := bdr.GroupUsage()
		So(err, ShouldBeNil)

		removeHistory(gu)

		groupExpectation := []*basedirs.Usage{
			{
				GID: 9000, UIDs: []uint32{8000, 8001, 8002, 8003}, Name: "9000", Owner: "BOM1", BaseDir: "/someDirectory/a/team1",
				UsageSize: 260, QuotaSize: 0, UsageInodes: 5, QuotaInodes: 0, Mtime: fixtimes.FixTime(time.Unix(33, 0)),
			},
			{
				GID: 9000, UIDs: []uint32{8000}, Name: "9000", Owner: "BOM1", BaseDir: "/someDirectory/c/team3",
				UsageSize: 30, UsageInodes: 2, Mtime: fixtimes.FixTime(time.Unix(66, 0)),
			},
			{
				GID: 9001, UIDs: []uint32{8010}, Name: "9001", Owner: "BOM2", BaseDir: "/someDirectory/a/mdt0/team2",
				UsageSize: 19092, UsageInodes: 3, Mtime: fixtimes.FixTime(time.Unix(48, 0)),
			},
			{
				GID: 9001, UIDs: []uint32{8010}, Name: "9001", Owner: "BOM2", BaseDir: "/someDirectory/b/team2",
				UsageSize: 200, UsageInodes: 2, QuotaInodes: 0, Mtime: fixtimes.FixTime(time.Unix(57, 0)),
			},
		}

		So(gu, ShouldResemble, groupExpectation)

		uu, err := bdr.UserUsage()
		So(err, ShouldBeNil)

		removeHistory(uu)

		userExpectation := []*basedirs.Usage{
			{
				UID: 8000, GIDs: []uint32{9000}, Name: "8000", BaseDir: "/someDirectory/a/team1", UsageSize: 10, QuotaSize: 0,
				UsageInodes: 1, QuotaInodes: 0, Mtime: fixtimes.FixTime(time.Unix(21, 0)),
			},
			{
				UID: 8000, GIDs: []uint32{9000}, Name: "8000", BaseDir: "/someDirectory/c/team3", UsageSize: 30, QuotaSize: 0,
				UsageInodes: 2, QuotaInodes: 0, Mtime: fixtimes.FixTime(time.Unix(66, 0)),
			},
			{
				UID: 8001, GIDs: []uint32{9000}, Name: "8001", BaseDir: "/someDirectory/a/team1", UsageSize: 50, QuotaSize: 0,
				UsageInodes: 1, QuotaInodes: 0, Mtime: fixtimes.FixTime(time.Unix(27, 0)),
			},
			{
				UID: 8002, GIDs: []uint32{9000}, Name: "8002", BaseDir: "/someDirectory/a/team1", UsageSize: 190, QuotaSize: 0,
				UsageInodes: 2, QuotaInodes: 0, Mtime: fixtimes.FixTime(time.Unix(33, 0)),
			},
			{
				UID: 8003, GIDs: []uint32{9000}, Name: "8003", BaseDir: "/someDirectory/a/team1", UsageSize: 10, QuotaSize: 0,
				UsageInodes: 1, QuotaInodes: 0, Mtime: fixtimes.FixTime(time.Unix(24, 0)),
			},
			{
				UID: 8010, GIDs: []uint32{9001}, Name: "8010", BaseDir: "/someDirectory/a/mdt0/team2", UsageSize: 19092,
				QuotaSize: 0, UsageInodes: 3, QuotaInodes: 0, Mtime: fixtimes.FixTime(time.Unix(48, 0)),
			},
			{
				UID: 8010, GIDs: []uint32{9001}, Name: "8010", BaseDir: "/someDirectory/b/team2", UsageSize: 200, QuotaSize: 0,
				UsageInodes: 2, QuotaInodes: 0, Mtime: fixtimes.FixTime(time.Unix(57, 0)),
			},
		}

		So(uu, ShouldResemble, userExpectation)
	})
}

func removeHistory(b []*basedirs.Usage) {
	for _, u := range b {
		u.DateNoFiles = time.Time{}
		u.DateNoSpace = time.Time{}
		u.Mtime = fixtimes.FixTime(u.Mtime)
	}
}

func TestTidy(t *testing.T) {
	Convey("For the tidy command, combine files within the source directory"+
		"are cleaned up and moved to the final directory", t, func() {
		srcDir := t.TempDir()
		finalDir := t.TempDir()

		for _, file := range [...]string{
			filepath.Join("a", "b", "combine.stats.gz"),
			filepath.Join("a", "b", "combine.byusergroup.gz"),
			filepath.Join("a", "b", "combine.bygroup"),
			filepath.Join("a", "b", "combine.log.gz"),
			filepath.Join("a", "b", "combine.dgut.db"),
			filepath.Join("test.dgut.dbs", "0", "dgut.db"),
			filepath.Join("test.dgut.dbs", "0", "dgut.db.children"),
			filepath.Join("test.dgut.dbs", "1", "dgut.db"),
			filepath.Join("test.dgut.dbs", "1", "dgut.db.children"),
			"basedirs.db",
			"basedirs.userusage.tsv",
			"basedirs.groupusage.tsv",
		} {
			fp := filepath.Join(srcDir, file)
			err := os.MkdirAll(filepath.Dir(fp), 0755)
			So(err, ShouldBeNil)

			writeFileString(t, fp, file)
		}

		_, _, jobs, err := runWRStat("tidy", "-d", "today", "-f", finalDir, srcDir)
		So(err, ShouldBeNil)

		So(len(jobs), ShouldEqual, 0)

		_, err = os.Lstat(srcDir)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEndWith, "no such file or directory")

		for file, expected := range map[string]string{
			"today_a.b.001.stats.gz":            filepath.Join("a", "b", "combine.stats.gz"),
			"today_a.b.001.bygroup":             filepath.Join("a", "b", "combine.bygroup"),
			"today_a.b.001.byusergroup.gz":      filepath.Join("a", "b", "combine.byusergroup.gz"),
			"today_a.b.001.logs.gz":             filepath.Join("a", "b", "combine.log.gz"),
			".dgut.dbs.updated":                 "",
			"today_001.dgut.dbs/0":              filepath.Join("a", "b", "combine.dgut.db"),
			"today_001.basedirs.userusage.tsv":  "basedirs.userusage.tsv",
			"today_001.basedirs.db":             "basedirs.db",
			"today_001.basedirs.groupusage.tsv": "basedirs.groupusage.tsv",
		} {
			f, err := os.Open(filepath.Join(finalDir, file))
			So(err, ShouldBeNil)

			contents, err := io.ReadAll(f)
			So(err, ShouldBeNil)

			f.Close()

			So(string(contents), ShouldEqual, expected)
		}
	})
}

const singDef = `Bootstrap: docker
From: golang:1.22-alpine
Stage: build

%setup

mkdir -p $SINGULARITY_ROOTFS/opt/wr
mkdir -p $SINGULARITY_ROOTFS/opt/wrstat

%post

apk add git make gcc musl-dev

git clone --branch v0.32.4 https://github.com/VertebrateResequencing/wr /opt/wr
cd /opt/wr
make install

cd /opt/wrstat
make installnonpm

Bootstrap: docker
From: alpine
Stage: final

%files from build

/go/bin/wr /usr/local/bin/wr
/go/bin/wrstat /usr/local/bin/wrstat

%setup

mkdir -p $SINGULARITY_ROOTFS/opt/wrstat

%post

apk add --no-cache coreutils

%runscript

stop() {
	wr manager stop

	exit ${1:-0}
}

waitForJobs() {
	until [ $(wr status | wc -l) -le 1 ]; do 
		if [ $(wr status -b | wc -l ) -gt 1 ]; then
			stop 1
		fi;

		sleep 1s
	done
}

mkdir -p /tmp/working/partial/
mkdir -p /tmp/working/complete/
mkdir -p /tmp/final/

WR_RunnerExecShell=sh wr manager start -s local --max_ram -1 --max_cores -1

wrstat multi -m 0 -p -w /tmp/working/partial/ /simple/*
waitForJobs

wrstat multi -m 0 -w /tmp/working/complete/ -f /tmp/final/ -l /tmp/working/partial -q /quota -o /owners /objects/*
waitForJobs

stop
`

func TestEnd2End(t *testing.T) {
	t.Parallel()

	if !commandExists("singularity") || !commandExists("sqfstar") {
		SkipConvey("need both 'singularity' and 'sqfstar' installed to run this test.", t, func() {})

		return
	}

	Convey("Test full end-2-end", t, func() {
		base := t.TempDir()
		def := filepath.Join(base, "singularity.def")
		sif := filepath.Join(base, "singularity.sif")
		users := filepath.Join(base, "passwd")
		groups := filepath.Join(base, "groups")
		files := filepath.Join(base, "files.sqfs")
		tmpTemp := t.TempDir()
		tmpHome := t.TempDir()

		writeFileString(t, def, singDef)

		wd, err := os.Getwd()
		So(err, ShouldBeNil)

		build := exec.Command("singularity", "build", "--fakeroot", "--bind", wd+":/opt/wrstat", sif, def)

		err = build.Run()
		So(err, ShouldBeNil)

		const (
			USERA = 20000
			USERB = 20001
			USERC = 20002
			USERD = 20003
			USERE = 20004

			GROUPA = 30000
			GROUPB = 30001
			GROUPC = 30002
			GROUPD = 30003
			GROUPE = 30004
		)

		root := newDir("/", 0, 0, false)

		root.Mkdir("objects", 0, 0)
		root.Mkdir("objects/store1", 0, 0)
		root.Mkdir("objects/store2", 0, 0)
		root.Mkdir("objects/store3", 0, 0)
		root.Mkdir("objects/store1/data", 0, 0)
		root.Mkdir("objects/store1/data/sheets", USERA, GROUPA)
		root.Create("objects/store1/data/sheets/doc1.txt", USERA, GROUPA, 2048)
		root.Create("objects/store1/data/sheets/doc2.txt", USERA, GROUPA, 8192)
		root.Mkdir("objects/store1/data/dbs", USERB, GROUPA)
		root.Create("objects/store1/data/dbs/dbA.db", USERB, GROUPA, 12345)
		root.Create("objects/store1/data/dbs/dbB.db", USERB, GROUPA, 54321)
		root.Mkdir("objects/store1/data/temp", USERC, GROUPA)
		root.Mkdir("objects/store1/data/temp/a", USERC, GROUPA)
		root.Create("objects/store1/data/temp/a/a.bed", USERC, GROUPA, 1000)
		root.Mkdir("objects/store1/data/temp/b", USERC, GROUPA)
		root.Create("objects/store1/data/temp/b/b.bed", USERC, GROUPA, 2000)
		root.Mkdir("objects/store1/data/temp/c", USERC, GROUPA)
		root.Create("objects/store1/data/temp/c/c.bed", USERC, GROUPA, 3000)
		root.Mkdir("objects/store2", 0, 0)
		root.Mkdir("objects/store2/part0", 0, 0)
		root.Mkdir("objects/store2/part1", 0, 0)
		root.Mkdir("objects/store2/part0/teams", 0, 0)
		root.Mkdir("objects/store2/part0/teams/team1", USERA, GROUPA)
		root.Mkdir("objects/store2/part0/teams/team2", USERB, GROUPB)
		root.Mkdir("objects/store2/part0/teams/team3", USERC, GROUPC)
		root.Mkdir("objects/store2/part1/other", USERD, GROUPA)
		root.Mkdir("objects/store2/part1/other/myDir", USERD, GROUPA)
		root.Mkdir("objects/store2/important", 0, 0)
		root.Mkdir("objects/store2/important/docs", USERB, GROUPD)

		root.Mkdir("simple", 0, 0)
		root.Mkdir("simple/A", USERA, GROUPA)
		root.Create("simple/A/a.file", USERA, GROUPA, 1)
		root.Mkdir("simple/E", USERE, GROUPE)
		root.Create("simple/E/b.tmp", USERE, GROUPE, 2)

		err = root.Write(files, "", "")
		So(err, ShouldBeNil)

		u, err := user.Current()
		So(err, ShouldBeNil)

		writeFileString(t, users, fmt.Sprintf(""+
			"root:x:0:0::/:/bin/sh\n"+
			"user:x:%[1]s:%[2]s::/:/bin/sh\n"+
			"U%[3]d:x:%[3]d:%[4]d::/:/bin/sh\n"+
			"U%[5]d:x:%[5]d:%[6]d::/:/bin/sh\n"+
			"U%[7]d:x:%[7]d:%[8]d::/:/bin/sh\n"+
			"U%[9]d:x:%[9]d:%[10]d::/:/bin/sh\n"+
			"U%[11]d:x:%[11]d:%[12]d::/:/bin/sh\n", u.Uid, u.Gid, USERA, GROUPA, USERB, GROUPB, USERC, GROUPC, USERD, GROUPD, USERE, GROUPE))

		writeFileString(t, groups, fmt.Sprintf(""+
			"root:x:0:\n"+
			"group:x:%[1]s:user::/:/bin/sh\n"+
			"G%[2]d:x:%[2]d:U%[3]d::/:/bin/sh\n"+
			"G%[4]d:x:%[4]d:U%[5]d::/:/bin/sh\n"+
			"G%[6]d:x:%[6]d:U%[7]d::/:/bin/sh\n"+
			"G%[8]d:x:%[8]d:U%[9]d::/:/bin/sh\n"+
			"G%[10]d:x:%[10]d:U%[11]d::/:/bin/sh\n", u.Gid, GROUPA, USERA, GROUPB, USERB, GROUPC, USERC, GROUPD, USERD, GROUPE, USERE))

		cmd := exec.Command("singularity", "run", "--bind", tmpTemp+":/tmp,"+users+":/etc/passwd,"+groups+":/etc/group", "--home", tmpHome, "--overlay", files, sif)

		So(cmd.Run(), ShouldBeNil)

		// fmt.Println("singularity", "shell", "--bind", tmpTemp+":/tmp,"+users+":/etc/passwd,"+groups+":/etc/group", "--home", tmpHome, "--overlay", files, sif)

		// time.Sleep(10 * time.Minute)

		userBaseDirs := fmt.Sprintf(``+
			"U%[1]d\t\t/objects/store1/data/sheets\t19954\t10240\t0\t2\t0\tOK\n"+
			"U%[1]d\t\t/simple/A\t19954\t1\t0\t1\t0\tOK\n"+
			"U%[2]d\t\t/objects/store1/data/dbs\t19954\t66666\t0\t2\t0\tOK\n"+
			"U%[3]d\t\t/objects/store1/data/temp\t19954\t6000\t0\t3\t0\tOK\n"+
			"U%[4]d\t\t/simple/E\t19954\t2\t0\t1\t0\tOK", USERA, USERB, USERC, USERE)

		ub, err := fs.Glob(os.DirFS(tmpTemp), filepath.Join("final", "*basedirs.userusage.tsv"))
		So(err, ShouldBeNil)
		So(len(ub), ShouldEqual, 1)

		compareFileContents(t, filepath.Join(tmpTemp, ub[0]), userBaseDirs)

		groupBaseDirs := fmt.Sprintf(``+
			"G%[1]d\t\t/objects/store1/data\t19954\t82906\t0\t7\t0\tNot OK\n"+
			"G%[1]d\t\t/simple/A\t19954\t1\t0\t1\t0\tNot OK\n"+
			"G%[2]d\t\t/simple/E\t19954\t2\t0\t1\t0\tNot OK", GROUPA, GROUPE)

		gb, err := fs.Glob(os.DirFS(tmpTemp), filepath.Join("final", "*basedirs.groupusage.tsv"))
		So(err, ShouldBeNil)
		So(len(gb), ShouldEqual, 1)

		compareFileContents(t, filepath.Join(tmpTemp, gb[0]), groupBaseDirs)
	})
}

var pseudoNow = time.Unix(0, 0)

func commandExists(exe string) bool {
	_, err := exec.LookPath(exe)

	return err == nil
}

type dir struct {
	dirs        map[string]*dir
	files       map[string]*tar.Header
	stickyGroup bool

	tar.Header
}

func newDir(name string, uid, gid int, sticky bool) *dir {
	return &dir{
		dirs:  make(map[string]*dir),
		files: make(map[string]*tar.Header),
		Header: tar.Header{
			Typeflag:   tar.TypeDir,
			Name:       name,
			Uid:        uid,
			Gid:        gid,
			Mode:       0777,
			ModTime:    pseudoNow,
			AccessTime: pseudoNow,
			ChangeTime: pseudoNow,
		},
		stickyGroup: sticky,
	}
}

func (d *dir) hasName(name string) bool {
	if _, ok := d.dirs[name]; ok {
		return true
	}

	if _, ok := d.files[name]; ok {
		return true
	}

	return false
}

func (d *dir) updateAccess() {
	pseudoNow = pseudoNow.Add(time.Second)
	d.AccessTime = pseudoNow
}

func (d *dir) updateMod() {
	d.ModTime = pseudoNow
}

func (d *dir) mkdir(name string, uid, gid int) *dir {
	d.updateAccess()

	if e, ok := d.dirs[name]; ok {
		return e
	} else if _, ok := d.files[name]; ok {
		return nil
	}

	d.updateMod()

	if d.stickyGroup {
		gid = d.Gid
	}

	e := newDir(filepath.Join(d.Name, name), uid, gid, d.stickyGroup)
	d.dirs[name] = e

	return e
}

func (d *dir) mkfile(name string, size int64, uid, gid int) bool {
	d.updateAccess()

	if d.hasName(name) {
		return false
	}

	d.updateMod()

	if d.stickyGroup {
		gid = d.Gid
	}

	d.files[name] = &tar.Header{
		Typeflag:   tar.TypeReg,
		Name:       filepath.Join(d.Name, name),
		Size:       size,
		Uid:        uid,
		Gid:        gid,
		Uname:      fmt.Sprintf("U%d", uid),
		Gname:      fmt.Sprintf("G%d", gid),
		Mode:       0777,
		ModTime:    pseudoNow,
		AccessTime: pseudoNow,
		ChangeTime: pseudoNow,
	}

	return true
}

func (d *dir) rmdir(name string) bool {
	d.updateAccess()

	if _, ok := d.dirs[name]; !ok {
		return false
	}

	d.updateMod()

	delete(d.dirs, name)

	return true
}

func (d *dir) rm(name string) bool {
	d.updateAccess()

	if _, ok := d.files[name]; !ok {
		return false
	}

	d.updateMod()

	delete(d.files, name)

	return true
}

func (d *dir) write(w *tar.Writer) error {
	if err := w.WriteHeader(&d.Header); err != nil {
		return err
	}

	for _, e := range d.dirs {
		if err := e.write(w); err != nil {
			return err
		}
	}

	for _, f := range d.files {
		if err := w.WriteHeader(f); err != nil {
			return err
		}

		zr := zeroReader(f.Size)

		if _, err := io.Copy(w, &zr); err != nil {
			return err
		}
	}

	return nil
}

func (d *dir) Mkdir(path string, uid, gid int) *dir {
	for _, part := range strings.Split(path, "/") {
		if d = d.mkdir(part, uid, gid); d == nil {
			break
		}
	}

	return d
}

func (d *dir) Create(path string, uid, gid int, size int64) bool {
	dir, file := filepath.Split(path)

	if d = d.Mkdir(strings.TrimSuffix(dir, "/"), uid, gid); d == nil {
		return false
	}

	return d.mkfile(file, size, uid, gid)
}

func (d *dir) RemoveDir(path string) bool {
	dir, file := filepath.Split(path)

	if d = d.Mkdir(dir, 0, 0); d == nil {
		return false
	}

	return d.rmdir(file)
}

func (d *dir) Remove(path string) bool {
	dir, file := filepath.Split(path)

	if d = d.Mkdir(dir, 0, 0); d == nil {
		return false
	}

	return d.rm(file)
}

func (d *dir) Write(path string, quota, owners string) (err error) {
	pr, pw := io.Pipe()
	cmd := exec.Command("sqfstar", path)
	cmd.Stdin = pr

	if err := cmd.Start(); err != nil {
		return err
	}

	tw := tar.NewWriter(pw)

	defer func() {
		for _, fn := range [...]func() error{
			tw.Close,
			pw.Close,
			cmd.Wait,
		} {
			if errr := fn(); errr != nil && err != nil {
				err = errr
			}
		}
	}()

	if err := d.write(tw); err != nil {
		return err
	}

	tw.WriteHeader(&tar.Header{
		Typeflag:   tar.TypeReg,
		Name:       "/quota",
		Size:       int64(len(quota)),
		Uid:        0,
		Gid:        0,
		Mode:       0777,
		ModTime:    pseudoNow,
		AccessTime: pseudoNow,
		ChangeTime: pseudoNow,
	})

	if _, err := io.WriteString(tw, quota); err != nil {
		return err
	}

	tw.WriteHeader(&tar.Header{
		Typeflag:   tar.TypeReg,
		Name:       "/owners",
		Size:       int64(len(owners)),
		Uid:        0,
		Gid:        0,
		Mode:       0777,
		ModTime:    pseudoNow,
		AccessTime: pseudoNow,
		ChangeTime: pseudoNow,
	})

	if _, err := io.WriteString(tw, owners); err != nil {
		return err
	}

	return nil
}

type zeroReader int

func (z *zeroReader) Read(p []byte) (int, error) {
	if *z == 0 {
		return 0, io.EOF
	}

	n := zeroReader(len(p))

	if n > *z {
		n = *z
	}

	*z -= zeroReader(n)

	return int(n), nil
}
