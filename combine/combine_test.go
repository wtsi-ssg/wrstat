package combine

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestWalk(t *testing.T) {
	Convey("The function combines and compresses the files", t, func() {
		testDir := t.TempDir()
		// here, err := os.Getwd()
		// testDir := filepath.Join(here, "test")
		// So(err, ShouldBeNil)

		buildOutputDir(t, testDir)

		Convey("And combine fails if there are no stat, group, user group, dgut and log files.", func() {
			pathSuffixes := [5]string{".stats", ".byusergroup", ".bygroup", ".dgut", ".log"}

			for _, suffix := range pathSuffixes {
				buildOutputDir(t, testDir)

				paths, errs := filepath.Glob(fmt.Sprintf("%s/*%s", testDir, suffix))
				So(errs, ShouldBeNil)

				for _, path := range paths {
					os.Remove(path)
				}

				err := combine(testDir)
				So(err, ShouldNotBeNil)
			}
		})
		Convey("And combine fails if the source dir does not exist.", func() {
			err := os.RemoveAll(testDir)
			So(err, ShouldBeNil)

			_, err = os.Stat(testDir)
			So(err, ShouldNotBeNil)

			err = combine(testDir)
			So(err, ShouldNotBeNil)
		})
		// Not sure if this function passes for the right reason?
		Convey("And combine fails if an incorrect relative path is supplied", func() {
			relDir := filepath.Join(testDir, "rel")
			err := os.MkdirAll(relDir, 448)
			So(err, ShouldBeNil)

			err = os.Chdir(relDir)
			So(err, ShouldBeNil)

			err = os.RemoveAll(relDir)
			So(err, ShouldBeNil)

			relDir += "../"

			err = combine(relDir)
			So(err, ShouldNotBeNil)
		})
		Convey(`And there exist the files combine.stats.gz, combine.byusergroup.gz, 
			combine.log.gz, combine.bygroup, combine.dgut.db at the root of output dir`, func() {
			err := combine(testDir)
			So(err, ShouldBeNil)

			expectedFiles := [5]string{"combine.stats.gz", "combine.byusergroup.gz", "combine.log.gz",
				"combine.bygroup", "combine.dgut.db"}

			for _, file := range expectedFiles {
				expectedFile := filepath.Join(testDir, file)
				_, err = os.Stat(expectedFile)
				So(err, ShouldBeNil)
			}
		})
		Convey("And the files have been properly compressed", func() {
			compressedFiles := [4]string{"combine.stats.gz", "combine.byusergroup.gz", "combine.log.gz", "ha.gz"}

			err := combine(testDir)
			So(err, ShouldBeNil)

			for _, file := range compressedFiles {
				f, err := ioutil.ReadFile((filepath.Join(testDir, file)))
				So(err, ShouldBeNil)

				expectedFileType := "application/x-gzip"
				fileType := http.DetectContentType(f)
				So(fileType, ShouldEqual, expectedFileType)
			}
		})
	})
}

func buildOutputDir(t *testing.T, outputDir string) {
	t.Helper()

	pathSuffixes := [5]string{".stats", ".byusergroup", ".bygroup", ".dgut", ".log"}

	for _, suffix := range pathSuffixes {
		f, err := os.Create(filepath.Join(outputDir, suffix))
		if err != nil {
			t.Fatal(err)
		}

		f.Close()
	}
}
