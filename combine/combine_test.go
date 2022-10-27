package combine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestWalk(t *testing.T) {
	Convey("The function combines and compresses the files", t, func() {
		testDir := t.TempDir()
		// testDir, err := os.Getwd()
		// So(err, ShouldBeNil)

		buildSrcDir(testDir)

		Convey("And combine fails if there are no stat, group, user group, dgut and log files.", func() {
			pathSuffixes := [5]string{".stats", ".byusergroup", ".bygroup", ".dgut", ".log"}

			for _, suffix := range pathSuffixes {
				buildSrcDir(testDir)

				paths, errs := filepath.Glob(fmt.Sprintf("%s/*%s", testDir, suffix))
				So(errs, ShouldBeNil)

				for _, path := range paths {
					os.Remove(path)
				}

				err := combineThis(testDir)
				So(err, ShouldNotBeNil)
			}
		})
		Convey("And the function fails if the source dir does not exist.", func() {
			os.RemoveAll(testDir)

			err := combineThis(testDir)
			So(err, ShouldNotBeNil)

			_, err = os.Stat(testDir)
			So(err, ShouldNotBeNil)
		})
		Convey("And the function fails if an incorrect relative path is supplied", func() {
			relDir := filepath.Join(testDir, "rel")
			err := os.MkdirAll(relDir, 448)
			So(err, ShouldBeNil)

			err = os.Chdir(relDir)
			So(err, ShouldBeNil)

			err = os.RemoveAll(relDir)
			So(err, ShouldBeNil)

			err = combineThis(testDir)
			So(err, ShouldNotBeNil)
		})
	})
}

func buildSrcDir(sourceDir string) {
	pathSuffixes := [5]string{".stats", ".byusergroup", ".bygroup", ".dgut", ".log"}
	for _, suffix := range pathSuffixes {
		file, err := os.Create(filepath.Join(sourceDir, suffix))
		file.Close()
		So(err, ShouldBeNil)
	}
}
