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

package scheduler

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/VertebrateResequencing/wr/jobqueue"
	jqs "github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	"github.com/inconshreveable/log15"
	"github.com/phayes/freeport"
	. "github.com/smartystreets/goconvey/convey"
)

const userOnlyPerm = 0700

func TestStatFile(t *testing.T) {
	deployment := "development"
	timeout := 10 * time.Second
	logger := log15.New()
	ctx := context.Background()

	Convey("You can get unique strings", t, func() {
		str := UniqueString()
		So(len(str), ShouldEqual, 20)

		str2 := UniqueString()
		So(len(str2), ShouldEqual, 20)
		So(str2, ShouldNotEqual, str)
	})

	Convey("When the jobqueue server is up", t, func() {
		config, d := prepareWrConfig(t)
		defer d()
		server := serve(t, config)
		defer server.Stop(ctx, true)

		Convey("You can make a Scheduler", func() {
			s, err := New(deployment, "", "", timeout, logger)
			So(err, ShouldBeNil)
			So(s, ShouldNotBeNil)

			wd, err := os.Getwd()
			So(err, ShouldBeNil)
			So(s.cwd, ShouldEqual, wd)

			exe, err := os.Executable()
			So(err, ShouldBeNil)
			So(s.Executable(), ShouldEqual, exe)

			So(s.jq, ShouldNotBeNil)

			Convey("which lets you create jobs", func() {
				job := s.NewJob("cmd", "rep", "req", "", "", nil)
				So(job.Cmd, ShouldEqual, "cmd")
				So(job.RepGroup, ShouldEqual, "rep")
				So(job.ReqGroup, ShouldEqual, "req")
				So(job.Cwd, ShouldEqual, wd)
				So(job.CwdMatters, ShouldBeTrue)
				So(job.Requirements, ShouldResemble, &jqs.Requirements{RAM: 100, Time: 10 * time.Second, Cores: 1, Disk: 1})
				So(job.Retries, ShouldEqual, 30)
				So(job.DepGroups, ShouldBeNil)
				So(job.Dependencies, ShouldBeNil)
				So(job.Override, ShouldEqual, 0)

				job2 := s.NewJob("cmd2", "rep", "req", "a", "b", nil)
				So(job2.Cmd, ShouldEqual, "cmd2")
				So(job2.DepGroups, ShouldResemble, []string{"a"})
				So(job2.Dependencies, ShouldResemble, jobqueue.Dependencies{{DepGroup: "b"}})

				Convey("which you can add to the queue", func() {
					err = s.SubmitJobs([]*jobqueue.Job{job, job2})
					So(err, ShouldBeNil)

					info := server.GetServerStats()
					So(info.Ready, ShouldEqual, 2)

					Convey("but you get an error if there are duplicates", func() {
						err = s.SubmitJobs([]*jobqueue.Job{job, job2})
						So(err, ShouldNotBeNil)
						So(err, ShouldEqual, errDupJobs)

						info := server.GetServerStats()
						So(info.Ready, ShouldEqual, 2)
					})
				})

				Convey("which you can't add to the queue if the server is down", func() {
					server.Stop(ctx, true)
					err = s.SubmitJobs([]*jobqueue.Job{job, job2})
					So(err, ShouldNotBeNil)
				})

				Convey("which you can't add to the queue if you disconnected", func() {
					err = s.Disconnect()
					So(err, ShouldBeNil)
					err = s.SubmitJobs([]*jobqueue.Job{job, job2})
					So(err, ShouldNotBeNil)
				})
			})
		})

		Convey("You can make a Scheduler with a specified cwd and it creates jobs in there", func() {
			cwd := t.TempDir()

			s, err := New(deployment, cwd, "", timeout, logger)
			So(err, ShouldBeNil)
			So(s, ShouldNotBeNil)

			job := s.NewJob("cmd", "rep", "req", "", "", nil)
			So(job.Cwd, ShouldEqual, cwd)
			So(job.CwdMatters, ShouldBeTrue)
		})

		Convey("You can't create a Scheduler in an invalid dir", func() {
			d := cdNonExistantDir(t)
			defer d()

			s, err := New(deployment, "", "", timeout, logger)
			So(err, ShouldNotBeNil)
			So(s, ShouldBeNil)
		})

		Convey("You can't create a Scheduler if you pass an invalid dir", func() {
			s, err := New(deployment, "/non_existent", "", timeout, logger)
			So(err, ShouldNotBeNil)
			So(s, ShouldBeNil)
		})

		Convey("You can make a Scheduler that creates sudo jobs", func() {
			s, err := New(deployment, "", "", timeout, logger)
			So(err, ShouldBeNil)
			So(s, ShouldNotBeNil)
			s.EnableSudo()

			job := s.NewJob("cmd", "rep", "req", "", "", nil)
			So(job.Cmd, ShouldEqual, "sudo cmd")
		})

		Convey("You can make a Scheduler with a Req override", func() {
			s, err := New(deployment, "", "", timeout, logger)
			So(err, ShouldBeNil)
			So(s, ShouldNotBeNil)

			req := DefaultRequirements()
			req.RAM = 16000

			job := s.NewJob("cmd", "rep", "req", "", "", req)
			So(job.Requirements.RAM, ShouldEqual, 16000)
			So(job.Override, ShouldEqual, 1)
		})

		Convey("You can make a Scheduler with a queue override", func() {
			s, err := New(deployment, "", "foo", timeout, logger)
			So(err, ShouldBeNil)
			So(s, ShouldNotBeNil)

			dreq := DefaultRequirements()

			job := s.NewJob("cmd", "rep", "req", "", "", nil)
			So(job.Requirements.RAM, ShouldEqual, dreq.RAM)
			So(job.Override, ShouldEqual, 0)
			So(job.Requirements.Other, ShouldResemble, map[string]string{"scheduler_queue": "foo"})
		})
	})

	Convey("When the jobqueue server is not up, you can't make a Scheduler", t, func() {
		_, d := prepareWrConfig(t)
		defer d()

		s, err := New(deployment, "", "", timeout, logger)
		So(err, ShouldNotBeNil)
		So(s, ShouldBeNil)
	})
}

// cdNonExistantDir changes directory to a temp directory, then deletes that
// directory. It returns a function you should defer to change back to your
// original directory.
func cdNonExistantDir(t *testing.T) func() {
	t.Helper()

	tmpDir, d := cdTmpDir(t)

	os.RemoveAll(tmpDir)

	return d
}

// cdTmpDir changes directory to a temp directory. It returns the path to the
// temp dir and a function you should defer to change back to your original
// directory. The tmp dir will be automatically deleted when tests end.
func cdTmpDir(t *testing.T) (string, func()) {
	t.Helper()

	tmpDir := t.TempDir()

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	err = os.Chdir(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	d := func() {
		err = os.Chdir(cwd)
		if err != nil {
			t.Logf("Chdir failed: %s", err)
		}
	}

	return tmpDir, d
}

// prepareWrConfig creates a temp directory, changes to that directory, creates
// a wr config file with available ports set, then returns a ServerConfig with
// that configuration. It also returns a function you should defer, which
// changes directory back.
func prepareWrConfig(t *testing.T) (jobqueue.ServerConfig, func()) {
	t.Helper()

	dir, d := cdTmpDir(t)

	clientPort, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("getting free port failed: %s", err)
	}

	webPort, err := freeport.GetFreePort()
	if err != nil {
		t.Fatalf("getting free port failed: %s", err)
	}

	managerDir := filepath.Join(dir, ".wr")
	managerDirActual := managerDir + "_development"

	err = os.MkdirAll(managerDirActual, userOnlyPerm)
	if err != nil {
		t.Fatal(err)
	}

	config := jobqueue.ServerConfig{
		Port:            fmt.Sprintf("%d", clientPort),
		WebPort:         fmt.Sprintf("%d", webPort),
		SchedulerName:   "local",
		SchedulerConfig: &jqs.ConfigLocal{Shell: "bash"},
		DBFile:          filepath.Join(managerDirActual, "db"),
		DBFileBackup:    filepath.Join(managerDirActual, "db_bk"),
		TokenFile:       filepath.Join(managerDirActual, "client.token"),
		CAFile:          filepath.Join(managerDirActual, "ca.pem"),
		CertFile:        filepath.Join(managerDirActual, "cert.pem"),
		CertDomain:      "localhost",
		KeyFile:         filepath.Join(managerDirActual, "key.pem"),
		Deployment:      "development",
	}

	f, err := os.Create(filepath.Join(dir, ".wr_config.yml"))
	if err != nil {
		t.Fatal(err)
	}

	configData := `managerport: "%s"
managerweb: "%s"
managerdir: "%s"`

	_, err = f.WriteString(fmt.Sprintf(configData, config.Port, config.WebPort, managerDir))
	if err != nil {
		t.Fatal(err)
	}

	return config, d
}

// serve calls Serve() but with a retry for 5s on failure. This allows time for
// a server that we recently stopped in a prior test to really not be listening
// on the ports any more.
func serve(t *testing.T, config jobqueue.ServerConfig) *jobqueue.Server {
	t.Helper()

	server, _, _, err := jobqueue.Serve(context.Background(), config)
	if err != nil {
		server, err = serveWithRetries(t, config)
	}

	if err != nil {
		t.Fatal(err)
	}

	return server
}

// serveWithRetries does the retrying part of serve().
func serveWithRetries(t *testing.T, config jobqueue.ServerConfig) (server *jobqueue.Server, err error) {
	t.Helper()

	limit := time.After(10 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			server, _, _, err = jobqueue.Serve(context.Background(), config)
			if err != nil {
				continue
			}

			ticker.Stop()

			return
		case <-limit:
			ticker.Stop()

			return
		}
	}
}
