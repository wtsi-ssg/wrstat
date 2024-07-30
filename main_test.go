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

const app = "wrstat"

func buildSelf() func() {
	cmd := exec.Command(
		"go", "build", "-tags", "netgo",
		"-ldflags=-X github.com/wtsi-ssg/wrstat/v4/cmd.runJobs=0 -X github.com/wtsi-ssg/wrstat/v4/cmd.Version=TESTVERSION",
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

	cmd := exec.Command("./wrstat", args...)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.ExtraFiles = append(cmd.ExtraFiles, pw)

	go func() {
		err = cmd.Run()

		pw.Close()
	}()

	jd := json.NewDecoder(pr)

	for {
		var j []*jobqueue.Job
		err := jd.Decode(&j)
		if err != nil {
			break
		}

		jobs = append(jobs, j...)
	}

	return stdout.String(), stderr.String(), jobs, err
}

func TestVersion(t *testing.T) {
	Convey("wrstat prints the correct version", t, func() {
		output, _, _, err := runWRStat("version")
		So(err, ShouldBeNil)
		So(strings.TrimSpace(output), ShouldEqual, "TESTVERSION")
	})
}

func TestCron(t *testing.T) {
	Convey("For the cron subcommand", t, func() {
		multiTests(t, "cron", "-c", "* * * * * *")
	})
}

func multiTests(t *testing.T, subcommand ...string) {
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
				Cmd:          fmt.Sprintf(" walk -n 1000000  -d %[1]s -o %[2]s/%[3]s/path/%[1]s -i wrstat-stat-path-%[4]s-%[3]s /some/path", walk1DepGroup, workingDir, repGroup, date),
				CwdMatters:   true,
				RepGroup:     fmt.Sprintf("wrstat-walk-path-%s-%s", date, repGroup),
				ReqGroup:     "wrstat-walk",
				Requirements: walkReqs,
				Override:     1,
				Retries:      30,
				DepGroups:    []string{walk1DepGroup},
			},
			{
				Cmd:          fmt.Sprintf(" walk -n 1000000  -d %[1]s -o %[2]s/%[3]s/path/%[1]s -i wrstat-stat-path-%[4]s-%[3]s /some-other/path", walk2DepGroup, workingDir, repGroup, date),
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
		_, _, jobs, err := runWRStat(append(subcommand, "-w", workingDir, "/some/path", "/some-other/path", "-f", "final_output", "-q", "quota_file", "-o", "owners_file")...)
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
				Cmd:          fmt.Sprintf(" walk -n 1000000  -d %[1]s -o %[2]s/%[3]s/path/%[1]s -i wrstat-stat-path-%[4]s-%[3]s /some/path", walk1DepGroup, workingDir, repGroup, date),
				CwdMatters:   true,
				RepGroup:     fmt.Sprintf("wrstat-walk-path-%s-%s", date, repGroup),
				ReqGroup:     "wrstat-walk",
				Requirements: walkReqs,
				Override:     1,
				Retries:      30,
				DepGroups:    []string{walk1DepGroup},
			},
			{
				Cmd:          fmt.Sprintf(" walk -n 1000000  -d %[1]s -o %[2]s/%[3]s/path/%[1]s -i wrstat-stat-path-%[4]s-%[3]s /some-other/path", walk2DepGroup, workingDir, repGroup, date),
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
				Cmd:          fmt.Sprintf(" basedir -q \"quota_file\" -o \"owners_file\"  \"%s/%s\" \"final_output\"", workingDir, repGroup),
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
		_, _, jobs, err := runWRStat(append(subcommand, "-l", "/path/to/partial_merge", "-w", workingDir, "/some/path", "/some-other/path", "-f", "final_output", "-q", "quota_file", "-o", "owners_file")...)
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
				Cmd:          fmt.Sprintf(" walk -n 1000000  -d %[1]s -o %[2]s/%[3]s/path/%[1]s -i wrstat-stat-path-%[4]s-%[3]s /some/path", walk1DepGroup, workingDir, repGroup, date),
				CwdMatters:   true,
				RepGroup:     fmt.Sprintf("wrstat-walk-path-%s-%s", date, repGroup),
				ReqGroup:     "wrstat-walk",
				Requirements: walkReqs,
				Override:     1,
				Retries:      30,
				DepGroups:    []string{walk1DepGroup},
			},
			{
				Cmd:          fmt.Sprintf(" walk -n 1000000  -d %[1]s -o %[2]s/%[3]s/path/%[1]s -i wrstat-stat-path-%[4]s-%[3]s /some-other/path", walk2DepGroup, workingDir, repGroup, date),
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
				Cmd:          fmt.Sprintf(" basedir -q \"quota_file\" -o \"owners_file\"  \"%s/%s\" \"final_output\"", workingDir, repGroup),
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
			f, err := os.Create(filepath.Join(tmp, file))
			So(err, ShouldBeNil)
			err = f.Close()
			So(err, ShouldBeNil)
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

				f, err := os.Create(path)
				So(err, ShouldBeNil)

				_, err = f.Write(make([]byte, stats.length))
				So(err, ShouldBeNil)

				err = f.Close()
				So(err, ShouldBeNil)
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

		atime := fi.Sys().(*syscall.Stat_t).Atim.Sec

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
			"dir.walk.byusergroup": userGroupExpectation,
			"dir.walk.dgut":        walkExpectations,
			"dir.walk.log":         "",
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
	Convey("", t, func() {
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
			f, err := os.Create(filepath.Join(tmp, file))
			So(err, ShouldBeNil)

			_, err = io.WriteString(f, contents)
			So(err, ShouldBeNil)

			err = f.Close()
			So(err, ShouldBeNil)
		}

		_, _, jobs, err := runWRStat("combine", tmp)
		So(err, ShouldBeNil)
		So(len(jobs), ShouldEqual, 0)

		for file, contents := range map[string]string{
			"combine.stats.gz":       "a\nb\nc\nd\ne\nf\ng\nh\n",
			"combine.byusergroup.gz": "a\t7\t8\nc\t1\t2\nb\t3\t4\na\t5\t6\nd\t9\t10\nc\t11\t12\nb\t13\t14\nf\t15\t16\ne\t17\t18\nb\t19\t20\n",
			"combine.bygroup":        "a\tb\tc\td\n1\t2\t3\t4\ne\tf\tg\th\n5\t6\t7\t8\n",
			"combine.log.gz":         "A log file\nwith 2 lines\nAnother log file, with 1 line\nLorem ipsum!!!!",
		} {
			f, err := os.Open(filepath.Join(tmp, file))
			So(err, ShouldBeNil)

			var r io.Reader

			if strings.HasSuffix(file, ".gz") {
				r, err = gzip.NewReader(f)
				So(err, ShouldBeNil)
			} else {
				r = f
			}

			buf, err := io.ReadAll(r)
			So(err, ShouldBeNil)

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

				f, err := os.Create(p)
				So(err, ShouldBeNil)

				_, err = io.WriteString(f, filepath.Join(dir, file))
				So(err, ShouldBeNil)

				err = f.Close()
				So(err, ShouldBeNil)

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
	Convey("", t, func() {
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
			f, err := os.Create(filepath.Join(configs, file))
			So(err, ShouldBeNil)

			_, err = io.WriteString(f, contents)
			So(err, ShouldBeNil)

			err = f.Close()
			So(err, ShouldBeNil)
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

		go o.Output(&stringCloser{WriteCloser: pw})

		err = db.Store(pr, 10000)
		So(err, ShouldBeNil)

		db.Close()

		_, _, jobs, err := runWRStat("basedir", "-q", filepath.Join(configs, "quota"), "-o", filepath.Join(configs, "owners"), "-b", filepath.Join(configs, "baseDirsConfig"), dbTmp, outputTmp)
		So(err, ShouldBeNil)
		So(len(jobs), ShouldEqual, 0)

		bdr, err := basedirs.NewReader(filepath.Join(dbTmp, "basedirs.db"), filepath.Join(configs, "owners"))
		So(err, ShouldBeNil)

		gu, err := bdr.GroupUsage()
		So(err, ShouldBeNil)

		removeHistory(gu)

		groupExpectation := []*basedirs.Usage{
			{GID: 9000, UIDs: []uint32{8000, 8001, 8002, 8003}, Name: "9000", Owner: "BOM1", BaseDir: "/someDirectory/a/team1", UsageSize: 260, QuotaSize: 0, UsageInodes: 5, QuotaInodes: 0, Mtime: fixtimes.FixTime(time.Unix(33, 0))},
			{GID: 9000, UIDs: []uint32{8000}, Name: "9000", Owner: "BOM1", BaseDir: "/someDirectory/c/team3", UsageSize: 30, UsageInodes: 2, Mtime: fixtimes.FixTime(time.Unix(66, 0))},
			{GID: 9001, UIDs: []uint32{8010}, Name: "9001", Owner: "BOM2", BaseDir: "/someDirectory/a/mdt0/team2", UsageSize: 19092, UsageInodes: 3, Mtime: fixtimes.FixTime(time.Unix(48, 0))},
			{GID: 9001, UIDs: []uint32{8010}, Name: "9001", Owner: "BOM2", BaseDir: "/someDirectory/b/team2", UsageSize: 200, UsageInodes: 2, QuotaInodes: 0, Mtime: fixtimes.FixTime(time.Unix(57, 0))},
		}

		So(gu, ShouldResemble, groupExpectation)

		uu, err := bdr.UserUsage()
		So(err, ShouldBeNil)

		removeHistory(uu)

		userExpectation := []*basedirs.Usage{
			{UID: 8000, GIDs: []uint32{9000}, Name: "8000", BaseDir: "/someDirectory/a/team1", UsageSize: 10, QuotaSize: 0, UsageInodes: 1, QuotaInodes: 0, Mtime: fixtimes.FixTime(time.Unix(21, 0))},
			{UID: 8000, GIDs: []uint32{9000}, Name: "8000", BaseDir: "/someDirectory/c/team3", UsageSize: 30, QuotaSize: 0, UsageInodes: 2, QuotaInodes: 0, Mtime: fixtimes.FixTime(time.Unix(66, 0))},
			{UID: 8001, GIDs: []uint32{9000}, Name: "8001", BaseDir: "/someDirectory/a/team1", UsageSize: 50, QuotaSize: 0, UsageInodes: 1, QuotaInodes: 0, Mtime: fixtimes.FixTime(time.Unix(27, 0))},
			{UID: 8002, GIDs: []uint32{9000}, Name: "8002", BaseDir: "/someDirectory/a/team1", UsageSize: 190, QuotaSize: 0, UsageInodes: 2, QuotaInodes: 0, Mtime: fixtimes.FixTime(time.Unix(33, 0))},
			{UID: 8003, GIDs: []uint32{9000}, Name: "8003", BaseDir: "/someDirectory/a/team1", UsageSize: 10, QuotaSize: 0, UsageInodes: 1, QuotaInodes: 0, Mtime: fixtimes.FixTime(time.Unix(24, 0))},
			{UID: 8010, GIDs: []uint32{9001}, Name: "8010", BaseDir: "/someDirectory/a/mdt0/team2", UsageSize: 19092, QuotaSize: 0, UsageInodes: 3, QuotaInodes: 0, Mtime: fixtimes.FixTime(time.Unix(48, 0))},
			{UID: 8010, GIDs: []uint32{9001}, Name: "8010", BaseDir: "/someDirectory/b/team2", UsageSize: 200, QuotaSize: 0, UsageInodes: 2, QuotaInodes: 0, Mtime: fixtimes.FixTime(time.Unix(57, 0))},
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
	Convey("For the tidy command, combine files within the source directory are cleaned up and moved to the final directory", t, func() {
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

			f, err := os.Create(fp)
			So(err, ShouldBeNil)

			_, err = io.WriteString(f, file)
			So(err, ShouldBeNil)

			err = f.Close()
			So(err, ShouldBeNil)
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
