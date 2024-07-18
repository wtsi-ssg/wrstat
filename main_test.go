package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/v4/cmd"
)

const app = "wrstat"

func buildSelf() func() {
	f, err := os.Open("cmd/root.go")
	if err != nil {
		failMainTest(err.Error())
	}

	code, err := io.ReadAll(f)
	if err != nil {
		failMainTest(err.Error())
	}

	f.Close()

	f, err = os.Create("cmd/root.go")
	if err != nil {
		failMainTest(err.Error())
	}

	_, err = f.Write(bytes.Replace(
		code,
		[]byte("func addJobsToQueue(s *scheduler.Scheduler, jobs []*jobqueue.Job) {"),
		[]byte("func addJobsToQueue(s *scheduler.Scheduler, jobs []*jobqueue.Job) {}\nfunc NOaddJobsToQueue(s *scheduler.Scheduler, jobs []*jobqueue.Job) {"),
		1,
	))
	if err != nil {
		failMainTest(err.Error())
	}

	f.Close()

	if err := exec.Command("make", "buildnonpm").Run(); err != nil {
		failMainTest(err.Error())

		return nil
	}

	return func() {
		os.Remove(app)
		f, _ = os.Create("cmd/root.go")
		f.Write(code)
		f.Close()
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

func TestVersion(t *testing.T) {
	Convey("wrstat prints the correct version", t, func() {
		run := exec.Command("./wrstat", "version")

		output, err := run.CombinedOutput()
		So(err, ShouldBeNil)
		So(strings.TrimSpace(string(output)), ShouldEqual, cmd.Version)
	})
}
