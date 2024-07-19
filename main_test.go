package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const app = "wrstat"

func buildSelf() func() {
	cmd := exec.Command(
		"go", "build", "-tags", "netgo", "-ldflags",
		"-X github.com/wtsi-ssg/wrstat/v4/cmd.jobRun=0 -X github.com/wtsi-ssg/wrstat/v4/cmd.Version=TESTVERSION",
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

func TestVersion(t *testing.T) {
	Convey("wrstat prints the correct version", t, func() {
		cmd := exec.Command("./wrstat", "version")

		output, err := cmd.CombinedOutput()
		So(err, ShouldBeNil)
		So(strings.TrimSpace(string(output)), ShouldEqual, "TESTVERSION")
	})
}
