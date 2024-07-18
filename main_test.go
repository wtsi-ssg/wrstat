package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/v4/cmd"
)

const app = "wrstat"

func buildSelf() func() {
	if err := exec.Command("make", "buildnonpm").Run(); err != nil {
		failMainTest(err.Error())

		return nil
	}

	return func() { os.Remove(app) }
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
	Convey("", t, func() {
		cd := exec.Command("./wrstat", "version")

		output, err := cd.CombinedOutput()
		So(err, ShouldBeNil)
		So(strings.TrimSpace(string(output)), ShouldEqual, cmd.Version)
	})
}
