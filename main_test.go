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
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/VertebrateResequencing/wr/client"
	wrcmd "github.com/VertebrateResequencing/wr/cmd"
	"github.com/VertebrateResequencing/wr/jobqueue"
	"github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	"github.com/inconshreveable/log15"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/spf13/pflag"
	"github.com/wtsi-ssg/wr/clog"
	"github.com/wtsi-ssg/wrstat/v6/cmd"
)

const (
	walkTime = 3 * time.Hour
	statTime = 3 * time.Hour
)

const app = "wrstat_test"

var (
	appWalk = app //nolint:gochecknoglobals
	appStat = app //nolint:gochecknoglobals
)

func buildSelf() func() {

	builds := map[string]string{app: "netgo"}

	if os.Getenv("WRSTAT_TEST_SPLIT") != "" {
		appWalk = app + "-walk"
		appStat = app + "-stat"

		builds = map[string]string{
			app:     "walk",
			appWalk: "walk,stat",
			appStat: "netgo,stat",
		}
	}

	for out, tags := range builds {
		cmd := exec.Command(
			"go", "build", "-tags", tags,
			"-ldflags=-X github.com/VertebrateResequencing/wr/client.PretendSubmissions=3 "+
				"-X github.com/wtsi-ssg/wrstat/v6/cmd.Version=TESTVERSION",
			"-o", out,
		)

		cmd.Env = append(os.Environ(), "CGO_ENABLED=1")
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		if err := cmd.Run(); err != nil {
			failMainTest(err.Error())

			return nil
		}
	}

	return func() {
		for out := range builds {
			os.Remove(out)
		}
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

func runWRStat(app string, args ...string) (string, string, []*jobqueue.Job, error) {
	var (
		stdout, stderr strings.Builder
		jobs           []*jobqueue.Job
	)

	cmd.RootCmd.SetOut(&stdout)
	cmd.RootCmd.SetErr(&stderr)
	cmd.RootCmd.SetArgs(args)

	for _, cmd := range cmd.RootCmd.Commands() {
		cmd.Flags().VisitAll(func(f *pflag.Flag) {
			f.Changed = false
			f.Value.Set(f.DefValue) //nolint:errcheck
			f.Changed = false
		})
	}

	cmd.InitLogger()

	pr, pw, err := os.Pipe()
	if err != nil {
		return "", "", nil, err
	}

	jd := json.NewDecoder(pr)
	done := make(chan struct{})

	go func() {
		defer pr.Close()
		defer close(done)

		for {
			var j []*jobqueue.Job

			if errr := jd.Decode(&j); errr != nil {
				return
			}

			jobs = append(jobs, j...)
		}
	}()

	client.PretendSubmissions = strconv.FormatInt(int64(pw.Fd()), 10)
	err = cmd.RootCmd.Execute()

	pw.Close()

	<-done

	client.PretendSubmissions = ""

	return stdout.String(), stderr.String(), jobs, err
}

func TestVersion(t *testing.T) {
	Convey("wrstat prints the correct version", t, func() {
		output, stderr, _, err := runWRStat(app, "version")
		So(err, ShouldBeNil)
		So(strings.TrimSpace(output), ShouldEqual, cmd.Version)
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

	Convey("'wrstat multi' command produces the correct jobs to run", func() {
		workingDir := t.TempDir()
		_, _, jobs, err := runWRStat(app, append(subcommand, "-w", workingDir, "/some/path", "/some-other/path",
			"-f", "final_output")...)
		So(err, ShouldBeNil)

		So(len(jobs), ShouldEqual, 6)
		So(len(jobs[0].DepGroups), ShouldEqual, 1)
		So(len(jobs[1].DepGroups), ShouldEqual, 1)
		So(len(jobs[0].RepGroup), ShouldBeGreaterThan, 20)

		expectation := createMultiJobExpectation(t, jobs, workingDir, 0, false)

		So(jobs, ShouldResemble, expectation)

		Convey("'wrstat multi' with a maximum run time produces jobs with the correct limit group", func() {
			_, _, jobs, err := runWRStat(app, append(subcommand, "-w", workingDir, "-t", "13", "/some/path", "/some-other/path",
				"-f", "final_output", "-l", "/path/for/logs", "-L", "/path/for/jobLogs")...)
			So(err, ShouldBeNil)

			expectation := createMultiJobExpectation(t, jobs, workingDir, 13, false)

			So(jobs, ShouldResemble, expectation)
		})

		Convey("The -b flag gets passed through to the walk subcommand", func() {
			_, _, jobs, err := runWRStat(app, append(subcommand, "-b", "-w", workingDir, "/some/path", "/some-other/path",
				"-f", "final_output")...)
			So(err, ShouldBeNil)

			expectation := createMultiJobExpectation(t, jobs, workingDir, 0, true)

			So(jobs, ShouldResemble, expectation)
		})
	})
}

func createMultiJobExpectation(t *testing.T, jobs []*jobqueue.Job, workingDir string,
	timeout int, statBlockSize bool,
) []*jobqueue.Job {
	t.Helper()

	walkReqs := &scheduler.Requirements{
		RAM:   16000,
		Time:  walkTime,
		Cores: 3,
		Disk:  1,
	}

	combineReqs := &scheduler.Requirements{
		RAM:   800,
		Time:  40 * time.Minute,
		Cores: 1,
		Disk:  1,
	}

	tidyReqs := &scheduler.Requirements{
		RAM:   100,
		Time:  10 * time.Second,
		Cores: 1,
		Disk:  1,
	}

	walk1DepGroup := jobs[0].DepGroups[0]
	walk2DepGroup := jobs[1].DepGroups[0]
	combine1DepGroup := jobs[2].DepGroups[0]
	combine2DepGroup := jobs[3].DepGroups[0]
	repGroup := jobs[0].RepGroup[len(jobs[0].RepGroup)-20:]
	statBlocks := ""

	if statBlockSize {
		statBlocks = " -b"
	}

	dateStr := regexp.MustCompile(`final_output/(\d\d\d\d\d\d\d\d-\d\d\d\d\d\d)_`).FindStringSubmatch(jobs[4].Cmd)
	So(len(dateStr), ShouldEqual, 2)

	_, err := time.Parse("20060102-150405", dateStr[1])
	So(err, ShouldBeNil)

	now := dateStr[1]

	exe, err := os.Executable()
	So(err, ShouldBeNil)

	exeWalk, err := filepath.Abs(appWalk)
	So(err, ShouldBeNil)

	var timeoutDate int64

	if timeout > 0 {
		timeoutStr := regexp.MustCompile(`-t (\d+) `).FindStringSubmatch(jobs[0].Cmd)
		So(len(timeoutStr), ShouldEqual, 2)

		timeoutDate, err = strconv.ParseInt(timeoutStr[1], 10, 0)
		So(err, ShouldBeNil)
		So(timeoutDate, ShouldAlmostEqual, time.Now().Unix()+int64(timeout)*3600, 5)
	}

	expectation := []*jobqueue.Job{
		{
			Cmd: fmt.Sprintf("%[5]s walk -n 1000000%[7]s -d %[1]s -t %[6]d -o %[2]s/%[3]s/%[4]s_／some／path -i"+
				" wrstat-stat-/some/path-%[4]s-%[3]s /some/path", walk1DepGroup,
				workingDir, repGroup, dateStr[1], exeWalk, timeoutDate, statBlocks),
			CwdMatters:   true,
			Cwd:          workingDir,
			RepGroup:     fmt.Sprintf("wrstat-walk-/some/path-%s-%s", dateStr[1], repGroup),
			ReqGroup:     "wrstat-walk",
			Requirements: walkReqs,
			Override:     1,
			Retries:      30,
			DepGroups:    []string{walk1DepGroup},
			State:        "delayed",
		},
		{
			Cmd: fmt.Sprintf("%[5]s walk -n 1000000%[7]s -d %[1]s -t %[6]d -o %[2]s/%[3]s/%[4]s_／some-other／path -i"+
				" wrstat-stat-/some-other/path-%[4]s-%[3]s /some-other/path", walk2DepGroup,
				workingDir, repGroup, dateStr[1], exeWalk, timeoutDate, statBlocks),
			CwdMatters:   true,
			Cwd:          workingDir,
			RepGroup:     fmt.Sprintf("wrstat-walk-/some-other/path-%s-%s", dateStr[1], repGroup),
			ReqGroup:     "wrstat-walk",
			Requirements: walkReqs,
			Override:     1,
			Retries:      30,
			DepGroups:    []string{walk2DepGroup},
			State:        "delayed",
		},
		{
			Cmd:          fmt.Sprintf("%s combine \"%s/%s/%s_／some／path\"", exe, workingDir, repGroup, dateStr[1]),
			CwdMatters:   true,
			Cwd:          workingDir,
			RepGroup:     fmt.Sprintf("wrstat-combine-/some/path-%s-%s", dateStr[1], repGroup),
			ReqGroup:     "wrstat-combine",
			Requirements: combineReqs,
			Override:     1,
			Retries:      30,
			DepGroups:    []string{combine1DepGroup},
			Dependencies: jobqueue.Dependencies{
				{
					DepGroup: walk1DepGroup,
				},
			},
			State: "delayed",
		},
		{
			Cmd:          fmt.Sprintf("%s combine \"%s/%s/%s_／some-other／path\"", exe, workingDir, repGroup, dateStr[1]),
			CwdMatters:   true,
			Cwd:          workingDir,
			RepGroup:     fmt.Sprintf("wrstat-combine-/some-other/path-%s-%s", dateStr[1], repGroup),
			ReqGroup:     "wrstat-combine",
			Requirements: combineReqs,
			Override:     1,
			Retries:      30,
			DepGroups:    []string{combine2DepGroup},
			Dependencies: jobqueue.Dependencies{
				{
					DepGroup: walk2DepGroup,
				},
			},
			State: "delayed",
		},
		{
			Cmd: fmt.Sprintf("%s tidy -f \"final_output/%s_／some／path\" \"%s/%s/%s_／some／path\"",
				exe, now, workingDir, repGroup, dateStr[1]),
			CwdMatters:   true,
			Cwd:          workingDir,
			RepGroup:     fmt.Sprintf("wrstat-tidy-/some/path-%s-%s", dateStr[1], repGroup),
			ReqGroup:     "wrstat-tidy",
			Requirements: tidyReqs,
			Override:     1,
			Retries:      30,
			Dependencies: jobqueue.Dependencies{
				{
					DepGroup: combine1DepGroup,
				},
			},
			State: "delayed",
		},
		{
			Cmd: fmt.Sprintf("%s tidy -f \"final_output/%s_／some-other／path\" \"%s/%s/%s_／some-other／path\"",
				exe, now, workingDir, repGroup, dateStr[1]),
			CwdMatters:   true,
			Cwd:          workingDir,
			RepGroup:     fmt.Sprintf("wrstat-tidy-/some-other/path-%s-%s", dateStr[1], repGroup),
			ReqGroup:     "wrstat-tidy",
			Requirements: tidyReqs,
			Override:     1,
			Retries:      30,
			Dependencies: jobqueue.Dependencies{
				{
					DepGroup: combine2DepGroup,
				},
			},
			State: "delayed",
		},
	}

	if timeout > 0 {
		finishTime := time.Unix(timeoutDate, 0).Format(time.DateTime)
		timeoutLimit := []string{"datetime<" + finishTime}

		for _, job := range expectation[:4] {
			job.LimitGroups = timeoutLimit
		}

		expectation = append(expectation, &jobqueue.Job{
			Cmd: fmt.Sprintf("%[1]s cleanup -w \"%[2]s/%[3]s\" -j %[3]q -l \"/path/for/logs\" "+
				"-r -L \"/path/for/jobLogs\"",
				exe, workingDir, repGroup),
			CwdMatters:   true,
			Cwd:          workingDir,
			RepGroup:     "wrstat-cleanup-" + dateStr[1],
			ReqGroup:     "wrstat-cleanup",
			Requirements: tidyReqs,
			LimitGroups:  []string{finishTime + "<datetime"},
			Override:     1,
			Retries:      30,
			State:        "delayed",
		})
	}

	return expectation
}

func TestMulti(t *testing.T) {
	Convey("For the multi subcommand", t, func() {
		multiTests(t, "multi")
	})
}

func TestMultiSingleRun(t *testing.T) {
	inTests = true

	Convey("For the multi subcommand when it disallows running multiple jobs for the same path", t, func() {
		temp := t.TempDir()

		t.Setenv("HOME", temp)

		wrcmd.RootCmd.SetArgs([]string{"manager", "start", "--deployment", "development", "-f"})

		go wrcmd.Execute()

		wrPath := filepath.Join(temp, ".wr_development")

		So(waitFor(10*time.Second, func() bool {
			_, err := os.Stat(wrPath)

			return err == nil
		}), ShouldBeTrue)

		var (
			c   *jobqueue.Client
			err error
		)

		waitFor(10*time.Second, func() bool {
			c, err = jobqueue.ConnectUsingConfig(clog.ContextWithLogHandler(context.Background(),
				log15.New().GetHandler()), "development", 10*time.Second)

			return err == nil
		})

		So(err, ShouldBeNil)

		Reset(func() { c.ShutdownServer() })

		testJobs := func(numJobs int, args ...string) {
			cmd.RootCmd.SetArgs(append([]string{
				"multi", "--deployment", "development", "-w", temp, "-f", "final_output",
			}, args...))

			So(cmd.RootCmd.Execute(), ShouldBeNil)

			jobs, err := c.GetIncomplete(100, "", false, false)
			So(err, ShouldBeNil)
			So(len(jobs), ShouldEqual, numJobs)
		}

		testJobs(3, "/some/path")
		testJobs(6, "/some/other/path")
		testJobs(6, "/some/path")
		testJobs(9, "/some/path", "/yet/another/path")
		testJobs(15, "-S", "/some/path", "/yet/another/path")
	})
}

func waitFor(wait time.Duration, fn func() bool) bool {
	start := time.Now()

	for time.Now().Add(-wait).Before(start) {
		if fn() {
			return true
		}

		time.Sleep(time.Second)
	}

	return false
}

func TestWalk(t *testing.T) {
	Convey("wrstat prints the correct output for a directory", t, func() {
		out := t.TempDir()
		tmp := t.TempDir()

		cwd, err := os.Getwd()
		So(err, ShouldBeNil)

		for _, dir := range [...]string{"/a/b/c/d/e", "/a/b/f", "/a/g/h"} {
			err := os.MkdirAll(filepath.Join(tmp, dir), 0755)
			So(err, ShouldBeNil)
		}

		for _, file := range [...]string{"/a/b/c/test.txt", "/a/b/f/tes\nt2.csv", "/a/test3"} {
			writeFileString(t, filepath.Join(tmp, file), "")
		}

		depgroup := "test-group"
		_, _, jobs, err := runWRStat(appWalk, "walk", tmp, "-o", out, "-d", depgroup, "-j", "1", "-i", "some-rep-group")
		So(err, ShouldBeNil)

		walk1 := filepath.Join(out, "walk.1")

		exe, err := filepath.Abs(appStat)
		So(err, ShouldBeNil)

		jobsExpectation := []*jobqueue.Job{
			{
				Cmd:         exe + " stat " + walk1,
				CwdMatters:  true,
				Cwd:         cwd,
				LimitGroups: []string{"wrstat-stat"},
				RepGroup:    "some-rep-group",
				ReqGroup:    "wrstat-stat",
				Requirements: &scheduler.Requirements{
					RAM:   200,
					Time:  statTime,
					Cores: 0.05,
					Disk:  1,
				},
				Override:  1,
				Retries:   30,
				DepGroups: []string{depgroup},
				State:     "delayed",
			},
		}

		So(jobs, ShouldResemble, jobsExpectation)

		expected := ""
		for _, subPath := range []string{
			"/", "/a/", "/a/b/", "/a/b/c/", "/a/b/c/d/", "/a/b/c/d/e/",
			"/a/b/c/test.txt", "/a/b/f/", "/a/b/f/tes\nt2.csv", "/a/g/", "/a/g/h/", "/a/test3",
		} {
			expected += strconv.Quote(tmp+subPath) + "\n"
		}

		compareFileContents(t, walk1, expected)

		_, _, jobs, err = runWRStat(appWalk, "walk", tmp, "-o", out, "-d", depgroup,
			"-j", "2", "--timeout", "100", "-i", "some-rep-group")
		So(err, ShouldBeNil)

		walk2 := filepath.Join(out, "walk.2")

		hundred := time.Unix(100, 0).Format(time.DateTime)

		jobsExpectation = []*jobqueue.Job{
			{
				Cmd:         exe + " stat " + walk1,
				CwdMatters:  true,
				Cwd:         cwd,
				LimitGroups: []string{"wrstat-stat", "datetime<" + hundred},
				RepGroup:    "some-rep-group",
				ReqGroup:    "wrstat-stat",
				Requirements: &scheduler.Requirements{
					RAM:   200,
					Time:  statTime,
					Cores: 0.05,
					Disk:  1,
				},
				Override:  1,
				Retries:   30,
				DepGroups: []string{depgroup},
				State:     "delayed",
			},
			{
				Cmd:         exe + " stat " + walk2,
				CwdMatters:  true,
				Cwd:         cwd,
				LimitGroups: []string{"wrstat-stat", "datetime<" + hundred},
				RepGroup:    "some-rep-group",
				ReqGroup:    "wrstat-stat",
				Requirements: &scheduler.Requirements{
					RAM:   200,
					Time:  statTime,
					Cores: 0.05,
					Disk:  1,
				},
				Override:  1,
				Retries:   30,
				DepGroups: []string{depgroup},
				State:     "delayed",
			},
		}

		So(jobs, ShouldResemble, jobsExpectation)

		Convey("The -b flag is passed through to the stat subcommand", func() {
			_, _, jobs, err = runWRStat(appWalk, "walk", tmp, "-b", "-o", out, "-d",
				depgroup, "-j", "2", "--timeout", "100", "-i", "some-rep-group")
			So(err, ShouldBeNil)

			jobsExpectation[0].Cmd = exe + " stat -b " + walk1
			jobsExpectation[1].Cmd = exe + " stat -b " + walk2

			So(jobs, ShouldResemble, jobsExpectation)
		})
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

	var r io.Reader = f

	if strings.HasSuffix(filename, ".gz") {
		r, err = gzip.NewReader(f)
		So(err, ShouldBeNil)
	}

	output, err := io.ReadAll(r)
	So(err, ShouldBeNil)

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	expectedLines := strings.Split(strings.TrimSpace(expectation), "\n")

	sort.Slice(lines, func(i, j int) bool { return lines[i] < lines[j] })
	sort.Slice(expectedLines, func(i, j int) bool { return expectedLines[i] < expectedLines[j] })

	if len(expectedLines) > 0 && strings.ContainsRune(expectedLines[0], 0) {
		So(len(lines), ShouldEqual, len(expectedLines))

		for n, line := range expectedLines {
			parts := strings.SplitN(line, "\x00", 2)
			So(len(parts), ShouldEqual, 2)
			So(lines[n], ShouldStartWith, parts[0])
		}
	} else {
		So(lines, ShouldResemble, expectedLines)
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

		for _, fileDefinition := range [...]File{
			{
				name:   "aDirectory/aFile\nfile",
				mtime:  time.Unix(minimumDate+7383773, 0),
				length: 10,
			},
			{
				name:  "aDirectory/aSubDirectory",
				mtime: time.Unix(minimumDate+314159, 0),
			},
			{
				name:  "aDirectory",
				mtime: time.Unix(minimumDate+133032, 0),
			},
			{
				name:  "anotherDirectory",
				mtime: time.Unix(minimumDate+282820, 0),
			},
			{
				name:  ".",
				mtime: time.Unix(minimumDate+271828, 0),
			},
		} {
			path := filepath.Join(tmp, fileDefinition.name)

			if fileDefinition.length > 0 {
				err := os.MkdirAll(filepath.Dir(path), 0755)
				So(err, ShouldBeNil)

				writeFileString(t, path, strings.Repeat("\x00", fileDefinition.length))
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

			err = os.Chtimes(path, time.Time{}, fileDefinition.mtime)
			So(err, ShouldBeNil)
		}

		workDir := t.TempDir()
		walkFilePath := filepath.Join(workDir, "dir.walk")
		walkFile, err := os.Create(walkFilePath)
		So(err, ShouldBeNil)

		err = fs.WalkDir(os.DirFS(tmp), ".", func(path string, _ fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			_, err = io.WriteString(walkFile, strconv.Quote(filepath.Join(tmp, path))+"\n")
			So(err, ShouldBeNil)

			return nil
		})
		So(err, ShouldBeNil)

		err = walkFile.Close()
		So(err, ShouldBeNil)

		_, _, jobs, err := runWRStat(appStat, "stat", walkFilePath)

		So(err, ShouldBeNil)
		So(len(jobs), ShouldEqual, 0)

		u, err := user.Current()
		So(err, ShouldBeNil)

		statsExpectation := fmt.Sprintf(""+
			"%[3]s\t4096\t%[1]s\t%[2]s\t%[14]d\t"+ct(271828)+"\t%[19]d\td\t%[8]d\t4\t%[13]d\t4096\n"+
			"%[4]s\t4096\t%[1]s\t%[2]s\t%[15]d\t"+ct(133032)+"\t%[20]d\td\t%[9]d\t3\t%[13]d\t4096\n"+
			"%[5]s\t10\t%[1]s\t%[2]s\t%[16]d\t"+ct(7383773)+"\t%[21]d\tf\t%[10]d\t1\t%[13]d\t10\n"+
			"%[6]s\t4096\t%[1]s\t%[2]s\t%[17]d\t"+ct(314159)+"\t%[22]d\td\t%[11]d\t2\t%[13]d\t4096\n"+
			"%[7]s\t4096\t%[1]s\t%[2]s\t%[18]d\t"+ct(282820)+"\t%[23]d\td\t%[12]d\t2\t%[13]d\t4096\n",
			u.Uid,
			u.Gid,
			strconv.Quote(tmp),
			strconv.Quote(filepath.Join(tmp, "aDirectory")),
			strconv.Quote(filepath.Join(tmp, "aDirectory", "aFile\nfile")),
			strconv.Quote(filepath.Join(tmp, "aDirectory", "aSubDirectory")),
			strconv.Quote(filepath.Join(tmp, "anotherDirectory")),
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

		f, err := os.Open(filepath.Join(workDir, "dir.walk.stats"))
		So(err, ShouldBeNil)

		data, err := io.ReadAll(f)
		f.Close()
		So(err, ShouldBeNil)
		So(string(data), ShouldEqual, statsExpectation)

		Convey("The -b flag prints the disk usage instead of the apprarent byte size", func() {
			_, _, jobs, err = runWRStat(appStat, "stat", "-b", walkFilePath)
			So(err, ShouldBeNil)

			statsExpectation := fmt.Sprintf(""+ //nolint:dupl
				"%[3]s\t4096\t%[1]s\t%[2]s\t%[14]d\t"+ct(271828)+"\t%[19]d\td\t%[8]d\t4\t%[13]d\t4096\n"+
				"%[4]s\t4096\t%[1]s\t%[2]s\t%[15]d\t"+ct(133032)+"\t%[20]d\td\t%[9]d\t3\t%[13]d\t4096\n"+
				"%[5]s\t4096\t%[1]s\t%[2]s\t%[16]d\t"+ct(7383773)+"\t%[21]d\tf\t%[10]d\t1\t%[13]d\t10\n"+
				"%[6]s\t4096\t%[1]s\t%[2]s\t%[17]d\t"+ct(314159)+"\t%[22]d\td\t%[11]d\t2\t%[13]d\t4096\n"+
				"%[7]s\t4096\t%[1]s\t%[2]s\t%[18]d\t"+ct(282820)+"\t%[23]d\td\t%[12]d\t2\t%[13]d\t4096\n",
				u.Uid,
				u.Gid,
				strconv.Quote(tmp),
				strconv.Quote(filepath.Join(tmp, "aDirectory")),
				strconv.Quote(filepath.Join(tmp, "aDirectory", "aFile\nfile")),
				strconv.Quote(filepath.Join(tmp, "aDirectory", "aSubDirectory")),
				strconv.Quote(filepath.Join(tmp, "anotherDirectory")),
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

			f, err := os.Open(filepath.Join(workDir, "dir.walk.stats"))
			So(err, ShouldBeNil)

			data, err := io.ReadAll(f)
			f.Close()
			So(err, ShouldBeNil)
			So(string(data), ShouldEqual, statsExpectation)
		})
	})
}

func TestCombine(t *testing.T) {
	Convey("For the combine subcommand, it creates the expected output", t, func() {
		tmp := t.TempDir()

		for file, contents := range map[string]string{
			"a.stats": "\"a\"\n\"b\"\n\"c\"\n",
			"b.stats": "\"d\"\n\"e\"\n\"f\"\n\"g\"\n",
			"c.stats": "\"h\"\n",
			"a.log":   "A log file\nwith 2 lines\n",
			"b.log":   "Another log file, with 1 line\n",
			"c.log":   "Lorem ipsum!!!!",
		} {
			writeFileString(t, filepath.Join(tmp, file), contents)
		}

		_, _, jobs, err := runWRStat(app, "combine", tmp)
		So(err, ShouldBeNil)
		So(len(jobs), ShouldEqual, 0)

		for file, contents := range map[string]string{
			"combine.stats.gz": "\"a\"\n\"b\"\n\"c\"\n\"d\"\n\"e\"\n\"f\"\n\"g\"\n\"h\"\n",
			"combine.log.gz":   "A log file\nAnother log file, with 1 line\nLorem ipsum!!!!\nwith 2 lines\n",
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
	})
}

func TestTidy(t *testing.T) {
	Convey("For the tidy command, combine files within the source directory "+
		"are cleaned up and moved to the final directory", t, func() {
		srcDir := t.TempDir()
		finalDir := t.TempDir()

		for _, file := range [...]string{
			"walk.1.log",
			"combine.stats.gz",
			"combine.log.gz",
		} {
			fp := filepath.Join(srcDir, file)
			err := os.MkdirAll(filepath.Dir(fp), 0755)
			So(err, ShouldBeNil)

			writeFileString(t, fp, file)
		}

		_, _, jobs, err := runWRStat(app, "tidy", "-f", finalDir, srcDir)
		So(err, ShouldBeNil)

		So(len(jobs), ShouldEqual, 0)

		_, err = os.Lstat(srcDir)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldEndWith, "no such file or directory")

		for file, expected := range map[string]string{
			"stats.gz": "combine.stats.gz",
			"logs.gz":  "combine.log.gz",
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

func TestCleanup(t *testing.T) {
	Convey("For the cleanup command, it moves logs and removes other artefacts", t, func() {
		const unique = "aUniquelyLengthedDir"

		working := t.TempDir()
		logs := t.TempDir()
		myDir := filepath.Join(working, unique)
		runDir := filepath.Join(myDir, "myRun")

		So(os.MkdirAll(runDir, 0755), ShouldBeNil)

		writeFileString(t, filepath.Join(runDir, "1.log"), "some log data")
		writeFileString(t, filepath.Join(runDir, "2.log"), "some more log data")

		_, _, _, err := runWRStat(app, "cleanup", "-l", logs, "-w", working)
		So(err, ShouldBeNil)

		compareFileContents(t, filepath.Join(logs, unique, "myRun", "1.log"), "some log data")
		compareFileContents(t, filepath.Join(logs, unique, "myRun", "2.log"), "some more log data")

		_, err = os.Stat(myDir)
		So(os.IsNotExist(err), ShouldBeTrue)
	})
}

const minimumDate = 315532801

func ct(n uint64) string {
	return strconv.FormatUint(n+minimumDate, 10)
}

func TestEnd2End(t *testing.T) {
	if !commandExists("singularity") || !commandExists("sqfstar") {
		SkipConvey("need both 'singularity' and 'sqfstar' installed to run this test.", t, func() {})

		return
	}

	Convey("Test full end-2-end", t, func() {
		base := t.TempDir()
		buildScript := filepath.Join(base, "build.sh")
		runScript := filepath.Join(base, "run.sh")
		sif := filepath.Join(base, "singularity.sif")
		users := filepath.Join(base, "passwd")
		groups := filepath.Join(base, "groups")
		files := filepath.Join(base, "files.sqfs")
		wrSrc := t.TempDir()
		binDir := t.TempDir()
		tmpTemp := t.TempDir()
		tmpHome := t.TempDir()

		buildSif := exec.Command("singularity", "build", sif, "docker://golang:latest")
		So(buildSif.Run(), ShouldBeNil)

		writeFileString(t, buildScript, `#!/bin/bash
set -euo pipefail
git clone --depth 1 --branch v0.34.0 https://github.com/VertebrateResequencing/wr /opt/wr &&
cd /opt/wr/ && GOPATH=/build/ go install;
cd /opt/wrstat && GOPATH=/build/ go install;
chmod -R +w /build;`)
		writeFileString(t, runScript, `#!/bin/bash

export PATH="/build/bin:$PATH";
export WR_MANAGERCERTDOMAIN="localhost";

stop() {
	wr manager stop;
}

trap stop EXIT;

waitForJobs() {
	while sleep 1s; do
		. <(wr status -o c -z -i "-" | sed -e '/^$/d' -e 's/: /=/' -e 's/^/declare /');
		. <(wr status -o c -i "wrstat-cleanup" | sed -e '/^$/d' -e 's/: /=/' -e 's/^/declare cleanup_/');
	
		if [ "$buried" -gt 0 ]; then
			echo "jobs failed";

			exit 1;
		fi;

		if [ "$cleanup_complete" -eq 1 ]; then
			echo "cleanup should not have run";

			exit 1;
		elif [ "$complete" -eq 20 ]; then
			break;
		fi;
	done;
}

getOpenPort() {
	read LOWERPORT UPPERPORT < /proc/sys/net/ipv4/ip_local_port_range
	declare PORT="";
	while true; do
		PORT="$(shuf -i $LOWERPORT-$UPPERPORT -n 1)";
		cat /proc/net/tcp | grep -q ":$(printf "%04X" $PORT) " || break;
	done;
	echo $PORT;
}

mkdir -p /tmp/working/partial/;
mkdir -p /tmp/working/complete/;
mkdir -p /tmp/final/;

export WR_ManagerPort=$(getOpenPort)
export WR_ManagerWeb=0

yes y | WR_RunnerExecShell=sh wr manager start -s local --max_ram -1 --max_cores -1;

wrstat multi -t 1 -m 0 -w /tmp/working/complete/ -f /tmp/final/ /simple/* /objects/*;
sleep 1s;
wrstat multi -t 1 -m 0 -w /tmp/working/complete/ -f /tmp/final/ /simple/* /objects/*;
waitForJobs;`)
		So(os.Chmod(buildScript, 0555), ShouldBeNil)
		So(os.Chmod(runScript, 0555), ShouldBeNil)

		wd, err := os.Getwd()
		So(err, ShouldBeNil)

		build := exec.Command( //nolint:gosec
			"singularity", "exec", "-e",
			"--bind", wrSrc+":/opt/wr,"+wd+":/opt/wrstat,"+binDir+":/build,"+buildScript+":/build.sh",
			"--home", tmpHome, sif, "/build.sh")

		err = build.Run()
		So(err, ShouldBeNil)

		const (
			UserA = 40000
			UserB = 40001
			UserC = 40002
			UserD = 40003
			UserE = 40004

			GroupA = 50000
			GroupB = 50001
			GroupC = 50002
			GroupD = 50003
			GroupE = 50004
		)

		root := newDir("/", 0, 0)

		root.Mkdir("objects", 0, 0)
		root.Mkdir("objects/store1", 0, 0)
		root.Mkdir("objects/store2", 0, 0)
		root.Mkdir("objects/store3", 0, 0)
		root.Mkdir("objects/store1/data", 0, 0)
		root.Mkdir("objects/store1/data/sheets", UserA, GroupA)
		root.Create("objects/store1/data/sheets/doc1.txt", UserA, GroupA, 2048)
		root.Create("objects/store1/data/sheets/doc2.txt", UserA, GroupA, 8192)
		root.Mkdir("objects/store1/data/dbs", UserB, GroupA)
		root.Create("objects/store1/data/dbs/dbA.db", UserB, GroupA, 12345)
		root.Create("objects/store1/data/dbs/dbB.db", UserB, GroupA, 54321)
		root.Mkdir("objects/store1/data/temp", UserC, GroupA)
		root.Mkdir("objects/store1/data/temp/a", UserC, GroupA)
		root.Create("objects/store1/data/temp/a/a.bed", UserC, GroupA, 1000)
		root.Mkdir("objects/store1/data/temp/b", UserC, GroupA)
		root.Create("objects/store1/data/temp/b/b.bed", UserC, GroupA, 2000)
		root.Mkdir("objects/store1/data/temp/c", UserC, GroupA)
		root.Create("objects/store1/data/temp/c/c.bed", UserC, GroupA, 3000)
		root.Mkdir("objects/store2", 0, 0)
		root.Mkdir("objects/store2/part0", 0, 0)
		root.Mkdir("objects/store2/part1", 0, 0)
		root.Mkdir("objects/store2/part0/teams", 0, 0)
		root.Mkdir("objects/store2/part0/teams/team1", UserA, GroupA)
		root.Create("objects/store2/part0/teams/team1/a.txt", UserA, GroupA, 100)
		root.Create("objects/store2/part0/teams/team1/b.txt", UserA, GroupB, 200)
		root.Mkdir("objects/store2/part0/teams/team2", UserB, GroupB)
		root.Create("objects/store2/part0/teams/team2/c.txt", UserB, GroupB, 1000)
		root.Create("objects/store2/part1/other.bed", UserD, GroupA, 999)
		root.Mkdir("objects/store2/part1/other", UserD, GroupA)
		root.Create("objects/store2/part1/other/my.tmp.gz", UserD, GroupD, 1024)
		root.Mkdir("objects/store2/part1/other/my\nDir", UserD, GroupA)
		root.Create("objects/store2/part1/other/my\nDir/my.tmp.old", UserD, GroupA, 2048)
		root.Create("objects/store2/part1/other/my\nDir/another.file", UserE, GroupB, 2048)
		root.Mkdir("objects/store2/important", 0, 0)
		root.Mkdir("objects/store2/important/docs\t", UserB, GroupD)
		root.Create("objects/store2/important/docs\t/my.doc", UserB, GroupD, 1200)
		root.Create("objects/store3/aFile", UserA, GroupA, 1024)

		root.Mkdir("simple", 0, 0)
		root.Mkdir("simple/A", UserA, GroupA)
		root.Create("simple/A/a.file", UserA, GroupA, 1)
		root.Mkdir("simple/E", UserE, GroupE)
		root.Create("simple/E/b.tmp", UserE, GroupE, 2)

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
			"U%[11]d:x:%[11]d:%[12]d::/:/bin/sh\n",
			u.Uid, u.Gid, UserA, GroupA, UserB, GroupB, UserC, GroupC, UserD, GroupD, UserE, GroupE))

		writeFileString(t, groups, fmt.Sprintf(""+
			"root:x:0:\n"+
			"group:x:%[1]s:user::/:/bin/sh\n"+
			"G%[2]d:x:%[2]d:U%[3]d::/:/bin/sh\n"+
			"G%[4]d:x:%[4]d:U%[5]d::/:/bin/sh\n"+
			"G%[6]d:x:%[6]d:U%[7]d::/:/bin/sh\n"+
			"G%[8]d:x:%[8]d:U%[9]d::/:/bin/sh\n"+
			"G%[10]d:x:%[10]d:U%[11]d::/:/bin/sh\n",
			u.Gid, GroupA, UserA, GroupB, UserB, GroupC, UserC, GroupD, UserD, GroupE, UserE))

		cmd := exec.Command("singularity", "exec", "-p", "-e", //nolint:gosec
			"--bind", tmpTemp+":/tmp,"+users+":/etc/passwd,"+groups+":/etc/group,"+
				binDir+":/build,"+runScript+":/run.sh",
			"--home", tmpHome,
			"--overlay", files, sif, "/run.sh")

		o, err := cmd.CombinedOutput()
		if err != nil {
			t.Logf("\nrun.sh failed: %s\n", string(o))
		}

		So(err, ShouldBeNil)

		for file, contents := range map[string]string{
			"*_／simple／A/logs.gz":       "",
			"*_／simple／E/logs.gz":       "",
			"*_／objects／store1/logs.gz": "",
			"*_／objects／store2/logs.gz": "",
			"*_／objects／store3/logs.gz": "",
			"*_／simple／A/stats.gz": fmt.Sprintf(""+
				strconv.Quote("/simple/A/a.file")+"\t1\t%[1]d\t%[2]d\t"+ct(166)+"\t"+ct(166)+"\t"+ct(166)+"\tf\t\x00\t1\t34\n"+
				strconv.Quote("/simple/A/")+"\t29\t%[1]d\t%[2]d\t"+ct(166)+"\t"+ct(166)+"\t"+ct(166)+"\td\t\x00\t2\t32",
				UserA, GroupA),
			"*_／simple／E/stats.gz": fmt.Sprintf(""+
				strconv.Quote("/simple/E/b.tmp")+"\t2\t%[1]d\t%[2]d\t"+ct(171)+"\t"+ct(171)+"\t"+ct(171)+"\tf\t\x00\t2\t34\n"+
				strconv.Quote("/simple/E/")+"\t28\t%[1]d\t%[2]d\t"+ct(171)+"\t"+ct(171)+"\t"+ct(171)+"\td\t\x00\t3\t32",
				UserE, GroupE),
			"*_／objects／store1/stats.gz": fmt.Sprintf(""+ //nolint:dupl
				strconv.Quote("/objects/store1/")+"\t27\t0\t0\t"+ct(10)+"\t"+
				ct(10)+"\t"+ct(10)+"\td\t\x00\t3\t32\n"+
				strconv.Quote("/objects/store1/data/")+"\t52\t0\t0\t"+ct(42)+"\t"+
				ct(42)+"\t"+ct(42)+"\td\t\x00\t5\t32\n"+
				strconv.Quote("/objects/store1/data/temp/")+"\t42\t%[1]d\t%[2]d\t"+
				ct(69)+"\t"+ct(69)+"\t"+ct(69)+"\td\t\x00\t5\t32\n"+
				strconv.Quote("/objects/store1/data/temp/c/c.bed")+"\t3000\t%[1]d\t%[2]d\t"+
				ct(75)+"\t"+ct(75)+"\t"+ct(75)+"\tf\t\x00\t1\t34\n"+
				strconv.Quote("/objects/store1/data/temp/c/")+"\t28\t%[1]d\t%[2]d\t"+
				ct(75)+"\t"+ct(75)+"\t"+ct(75)+"\td\t\x00\t2\t32\n"+
				strconv.Quote("/objects/store1/data/dbs/dbA.db")+"\t12345\t%[3]d\t%[2]d\t"+
				ct(33)+"\t"+ct(33)+"\t"+ct(33)+"\tf\t\x00\t1\t34\n"+
				strconv.Quote("/objects/store1/data/dbs/dbB.db")+"\t54321\t%[3]d\t%[2]d\t"+
				ct(38)+"\t"+ct(38)+"\t"+ct(38)+"\tf\t\x00\t1\t34\n"+
				strconv.Quote("/objects/store1/data/dbs/")+"\t43\t%[3]d\t%[2]d\t"+ct(38)+"\t"+
				ct(38)+"\t"+ct(38)+"\td\t\x00\t2\t32\n"+
				strconv.Quote("/objects/store1/data/sheets/doc1.txt")+"\t2048\t%[4]d\t%[2]d\t"+
				ct(19)+"\t"+ct(19)+"\t"+ct(19)+"\tf\t\x00\t1\t34\n"+
				strconv.Quote("/objects/store1/data/sheets/doc2.txt")+"\t8192\t%[4]d\t%[2]d\t"+
				ct(24)+"\t"+ct(24)+"\t"+ct(24)+"\tf\t\x00\t1\t34\n"+
				strconv.Quote("/objects/store1/data/sheets/")+"\t47\t%[4]d\t%[2]d\t"+
				ct(24)+"\t"+ct(24)+"\t"+ct(24)+"\td\t\x00\t2\t32\n"+
				strconv.Quote("/objects/store1/data/temp/a/a.bed")+"\t1000\t%[1]d\t%[2]d\t"+
				ct(53)+"\t"+ct(53)+"\t"+ct(53)+"\tf\t\x00\t1\t34\n"+
				strconv.Quote("/objects/store1/data/temp/a/")+"\t28\t%[1]d\t%[2]d\t"+
				ct(53)+"\t"+ct(53)+"\t"+ct(53)+"\td\t\x00\t2\t32\n"+
				strconv.Quote("/objects/store1/data/temp/b/b.bed")+"\t2000\t%[1]d\t%[2]d\t"+
				ct(64)+"\t"+ct(64)+"\t"+ct(64)+"\tf\t\x00\t1\t34\n"+
				strconv.Quote("/objects/store1/data/temp/b/")+"\t28\t%[1]d\t%[2]d\t"+
				ct(64)+"\t"+ct(64)+"\t"+ct(64)+"\td\t\x00\t2\t32",
				UserC, GroupA, UserB, UserA),
			"*_／objects／store2/stats.gz": fmt.Sprintf(""+ //nolint:dupl
				strconv.Quote("/objects/store2/")+"\t58\t0\t0\t"+ct(148)+"\t"+
				ct(148)+"\t"+ct(148)+"\td\t\x00\t5\t32\n"+
				strconv.Quote("/objects/store2/part1/other.bed")+"\t999\t%[1]d\t%[2]d\t"+
				ct(119)+"\t"+ct(119)+"\t"+ct(119)+"\tf\t\x00\t1\t34\n"+
				strconv.Quote("/objects/store2/part1/")+"\t45\t0\t0\t"+ct(123)+"\t"+
				ct(123)+"\t"+ct(123)+"\td\t\x00\t3\t32\n"+
				strconv.Quote("/objects/store2/part1/other/my.tmp.gz")+"\t1024\t%[1]d\t%[3]d\t"+
				ct(128)+"\t"+ct(128)+"\t"+ct(128)+"\tf\t\x00\t1\t34\n"+
				strconv.Quote("/objects/store2/part1/other/")+"\t46\t%[1]d\t%[2]d\t"+
				ct(133)+"\t"+ct(133)+"\t"+ct(133)+"\td\t\x00\t3\t32\n"+
				strconv.Quote("/objects/store2/part1/other/my\nDir/my.tmp.old")+
				"\t2048\t%[1]d\t%[2]d\t"+ct(139)+"\t"+ct(139)+"\t"+ct(139)+"\tf\t\x00\t1\t34\n"+
				strconv.Quote("/objects/store2/part1/other/my\nDir/another.file")+
				"\t2048\t%[7]d\t%[5]d\t"+ct(145)+"\t"+ct(145)+"\t"+ct(145)+"\tf\t\x00\t1\t34\n"+
				strconv.Quote("/objects/store2/part1/other/my\nDir/")+"\t53\t%[1]d\t%[2]d\t"+
				ct(145)+"\t"+ct(145)+"\t"+ct(145)+"\td\t\x00\t2\t32\n"+
				strconv.Quote("/objects/store2/important/")+"\t28\t0\t0\t"+ct(152)+
				"\t"+ct(152)+"\t"+ct(152)+"\td\t\x00\t3\t32\n"+
				strconv.Quote("/objects/store2/important/docs\t/my.doc")+
				"\t1200\t%[4]d\t%[3]d\t"+ct(157)+"\t"+ct(157)+"\t"+ct(157)+
				"\tf\t\x00\t1\t34\n"+
				strconv.Quote("/objects/store2/important/docs\t/")+"\t29\t%[4]d\t%[3]d\t"+
				ct(157)+"\t"+ct(157)+"\t"+ct(157)+"\td\t\x00\t2\t32\n"+
				strconv.Quote("/objects/store2/part0/")+"\t28\t0\t0\t"+ct(87)+"\t"+
				ct(87)+"\t"+ct(87)+"\td\t\x00\t3\t32\n"+
				strconv.Quote("/objects/store2/part0/teams/")+"\t41\t0\t0\t"+ct(109)+
				"\t"+ct(109)+"\t"+ct(109)+"\td\t\x00\t4\t32\n"+
				strconv.Quote("/objects/store2/part0/teams/team2/c.txt")+
				"\t1000\t%[4]d\t%[5]d\t"+ct(115)+"\t"+ct(115)+"\t"+ct(115)+"\tf\t\x00\t1\t34\n"+
				strconv.Quote("/objects/store2/part0/teams/team2/")+"\t28\t%[4]d\t%[5]d\t"+
				ct(115)+"\t"+ct(115)+"\t"+ct(115)+"\td\t\x00\t2\t32\n"+
				strconv.Quote("/objects/store2/part0/teams/team1/a.txt")+"\t100\t%[6]d\t%[2]d\t"+
				ct(98)+"\t"+ct(98)+"\t"+ct(98)+"\tf\t\x00\t1\t34\n"+
				strconv.Quote("/objects/store2/part0/teams/team1/b.txt")+
				"\t200\t%[6]d\t%[5]d\t"+ct(104)+"\t"+ct(104)+"\t"+ct(104)+"\tf\t\x00\t1\t34\n"+
				strconv.Quote("/objects/store2/part0/teams/team1/")+"\t41\t%[6]d\t%[2]d\t"+
				ct(104)+"\t"+ct(104)+"\t"+ct(104)+"\td\t\x00\t2\t32",
				UserD, GroupA, GroupD, UserB, GroupB, UserA, UserE),
			"*_／objects／store3/stats.gz": fmt.Sprintf(""+
				strconv.Quote("/objects/store3/aFile")+"\t1024\t%d\t%d\t"+ct(160)+"\t"+ct(160)+"\t"+ct(160)+"\tf\t\x00\t1\t34\n"+
				strconv.Quote("/objects/store3/")+"\t28\t0\t0\t"+ct(160)+"\t"+ct(160)+"\t"+ct(160)+"\td\t\x00\t2\t32",
				UserA, GroupA),
		} {
			files, errr := fs.Glob(os.DirFS(tmpTemp), filepath.Join("final", file))
			So(errr, ShouldBeNil)

			if len(files) != 1 {
				t.Logf("\nfile [%s] found %d times\n", file, len(files))
			}

			So(len(files), ShouldEqual, 1)

			compareFileContents(t, filepath.Join(tmpTemp, files[0]), contents)
		}
	})
}

var pseudoNow = time.Unix(minimumDate, 0) //nolint:gochecknoglobals

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

func newDir(name string, uid, gid int) *dir {
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

	e := newDir(filepath.Join(d.Name, name), uid, gid)
	d.dirs[name] = e

	return e
}

func (d *dir) mkfile(name string, size int64, uid, gid int) {
	d.updateAccess()

	if d.hasName(name) {
		return
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
}

func (d *dir) write(w *tar.Writer) error {
	if err := w.WriteHeader(&d.Header); err != nil {
		return err
	}

	for _, k := range sortKeys(d.dirs) {
		e := d.dirs[k]

		if err := e.write(w); err != nil {
			return err
		}
	}

	for _, k := range sortKeys(d.files) {
		f := d.files[k]

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

func sortKeys[Map ~map[string]V, V any](m Map) []string {
	keys := make([]string, 0, len(m))

	for k := range m {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	return keys
}

func (d *dir) Mkdir(path string, uid, gid int) *dir {
	for _, part := range strings.Split(path, "/") {
		if d = d.mkdir(part, uid, gid); d == nil {
			break
		}
	}

	return d
}

func (d *dir) Create(path string, uid, gid int, size int64) {
	dir, file := filepath.Split(path)

	if d = d.Mkdir(strings.TrimSuffix(dir, "/"), uid, gid); d != nil {
		d.mkfile(file, size, uid, gid)
	}
}

func (d *dir) Write(path string, quota, owners string) (err error) {
	pr, pw := io.Pipe()
	cmd := exec.Command("sqfstar", path)
	cmd.Stdin = pr

	if err = cmd.Start(); err != nil {
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

	if err := tw.WriteHeader(&tar.Header{
		Typeflag:   tar.TypeReg,
		Name:       "/quota",
		Size:       int64(len(quota)),
		Uid:        0,
		Gid:        0,
		Mode:       0777,
		ModTime:    pseudoNow,
		AccessTime: pseudoNow,
		ChangeTime: pseudoNow,
	}); err != nil {
		return err
	}

	if _, err := io.WriteString(tw, quota); err != nil {
		return err
	}

	if err := tw.WriteHeader(&tar.Header{
		Typeflag:   tar.TypeReg,
		Name:       "/owners",
		Size:       int64(len(owners)),
		Uid:        0,
		Gid:        0,
		Mode:       0777,
		ModTime:    pseudoNow,
		AccessTime: pseudoNow,
		ChangeTime: pseudoNow,
	}); err != nil {
		return err
	}

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

	*z -= n

	return int(n), nil
}
