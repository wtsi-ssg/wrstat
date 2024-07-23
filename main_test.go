package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/VertebrateResequencing/wr/jobqueue"
	"github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	. "github.com/smartystreets/goconvey/convey"
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

		_, _, _, err = runWRStat("stat", statFilePath)

		So(err, ShouldBeNil)

		u, err := user.Current()
		So(err, ShouldBeNil)

		expectation := fmt.Sprintf(""+
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

		f, err := os.Open(filepath.Join(statDir, "dir.walk.stats"))
		So(err, ShouldBeNil)

		data, err := io.ReadAll(f)
		f.Close()
		So(err, ShouldBeNil)

		So(string(data), ShouldEqual, expectation)
	})
}

func TestWalk(t *testing.T) {
	//TODO: Seems to be sporadically inconsistant, possible race issue.

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

		compareFileContents(t, filepath.Join(out, "walk.1"), fmt.Sprintf(`%[1]s
%[1]s/a
%[1]s/a/g/h
%[1]s/a/b/f/test2.csv
%[1]s/a/b/c/test.txt
%[1]s/a/b/c/d`, tmp))

		compareFileContents(t, filepath.Join(out, "walk.2"), fmt.Sprintf(`%[1]s/a/test3
%[1]s/a/g
%[1]s/a/b
%[1]s/a/b/f
%[1]s/a/b/c
%[1]s/a/b/c/d/e`, tmp))
	})
}

func compareFileContents(t *testing.T, filename, expectation string) {
	t.Helper()

	f, err := os.Open(filename)
	So(err, ShouldBeNil)

	defer f.Close()

	output, err := io.ReadAll(f)
	So(err, ShouldBeNil)
	So(strings.TrimSpace(string(output)), ShouldEqual, expectation)
}

func removeJobRepGroupSuffixes(jobs []*jobqueue.Job) {
	for _, job := range jobs {
		job.RepGroup = job.RepGroup[:len(job.RepGroup)-21]
	}
}

func TestMulti(t *testing.T) {
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

	Convey("wrstat gets the stats for a partial run", t, func() {
		workingDir := t.TempDir()
		_, _, jobs, err := runWRStat("multi", "-w", workingDir, "-p", "/some/path", "/some-other/path")
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

	Convey("wrstat gets the stats for a normal run", t, func() {
		workingDir := t.TempDir()
		_, _, jobs, err := runWRStat("multi", "-w", workingDir, "/some/path", "/some-other/path", "-f", "final_output", "-q", "quota_file", "-o", "owners_file")
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

	Convey("wrstat gets the stats for a normal run with a partial merge", t, func() {
		workingDir := t.TempDir()
		_, _, jobs, err := runWRStat("multi", "-l", "/path/to/partial_merge", "-w", workingDir, "/some/path", "/some-other/path", "-f", "final_output", "-q", "quota_file", "-o", "owners_file")
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
