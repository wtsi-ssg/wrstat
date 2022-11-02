package combine

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestCombine(t *testing.T) { //nolint:gocognit
	Convey("The function combines the files", t, func() {
		testDir := t.TempDir()

		test := Combine{
			SourceDir: testDir,

			Suffixes: map[string]string{
				".byusergroup": "combine.byusergroup.gz",
				".log":         "combine.log.gz",
				".bygroup":     "combine.bygroup",
			},

			Functions: [3]mergeStreamToOutputFunc{mergeUserGroupStreamToCompressedFile,
				mergeLogStreamToCompressedFile, mergeGroupStreamToFile},
		}

		/*here, err := os.Getwd()
		So(err, ShouldBeNil)
		testDir := filepath.Join(here, "test")
		err = os.MkdirAll(testDir, 448)
		So(err, ShouldBeNil)*/

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

				err := test.Combine()
				So(err, ShouldNotBeNil)
			}
		})
		Convey("And combine fails if the source dir does not exist.", func() {
			err := os.RemoveAll(testDir)
			So(err, ShouldBeNil)

			_, err = os.Stat(testDir)
			So(err, ShouldNotBeNil)

			err = test.Combine()
			So(err, ShouldNotBeNil)
		})
		Convey("And combine fails if an incorrect relative path is supplied", func() {
			relDir := filepath.Join(testDir, "rel")
			err := os.MkdirAll(relDir, 448)
			So(err, ShouldBeNil)

			err = os.Chdir(relDir)
			So(err, ShouldBeNil)

			err = os.RemoveAll(relDir)
			So(err, ShouldBeNil)

			relDir += "../"

			test.SourceDir = relDir
			err = test.Combine()
			So(err, ShouldNotBeNil)
		})
		Convey(`And there exist the files combine.stats.gz, combine.byusergroup.gz,
			combine.log.gz, combine.bygroup, combine.dgut.db at the root of output dir`, func() {
			err := test.Combine()
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
			compressedFiles := [3]string{"combine.stats.gz", "combine.byusergroup.gz", "combine.log.gz"}

			err := test.Combine()
			So(err, ShouldBeNil)

			for _, file := range compressedFiles {
				f, err := os.ReadFile((filepath.Join(testDir, file)))
				So(err, ShouldBeNil)

				expectedFileType := "application/x-gzip"
				fileType := http.DetectContentType(f)
				So(fileType, ShouldEqual, expectedFileType)
			}
		})
		Convey(`And combine.stats.gz, combine.log.gz, combine.byusergroup.gz contain the merging or 
			concatenation of their corresponding input files.`, func() {
			inputOutputSuffixes := map[string]string{
				".stats":       "combine.stats.gz",
				".log":         "combine.log.gz",
				".byusergroup": "combine.byusergroup.gz"}

			for inputSuffix, outputSuffix := range inputOutputSuffixes {
				expectedOutputPath := filepath.Join(testDir, outputSuffix)

				expectedFileContents := writeToTestFiles(t, testDir, inputSuffix)

				err := test.Combine()
				So(err, ShouldBeNil)

				actualFile, err := os.Open(expectedOutputPath)
				So(err, ShouldBeNil)

				actualFileReader, err := gzip.NewReader(actualFile)
				So(err, ShouldBeNil)
				defer actualFileReader.Close()

				actualFileScanner := bufio.NewScanner(actualFileReader)

				var actualFileContents string
				for actualFileScanner.Scan() {
					actualFileContents += actualFileScanner.Text()
				}

				So(actualFileContents, ShouldEqual, expectedFileContents)
			}
		})
		Convey("And combine.bygroup contains the merged contents of the .bygroup files.", func() {
			expectedOutputPath := filepath.Join(testDir, "combine.bygroup")

			expectedFileContents := writeToTestFiles(t, testDir, ".bygroup")

			err := test.Combine()
			So(err, ShouldBeNil)

			actualFile, err := os.ReadFile(expectedOutputPath)
			So(err, ShouldBeNil)

			actualFileContents := strings.ReplaceAll(string(actualFile), "\n", "")

			So(actualFileContents, ShouldEqual, expectedFileContents)
		})
		// Convey("And the dgut file contains the right stuff. -- FILL LATER -- ", func () {

		// })
	})
}

// buildOutputDir builds a directory within the provided testing environment.
func buildOutputDir(t *testing.T, outputDir string) {
	t.Helper()

	pathSuffixes := [10]string{"walk.1.stats", "walk.2.stats",
		"walk.1.byusergroup", "walk.2.byusergroup", "walk.1.bygroup",
		"walk.2.bygroup", "walk.1.dgut", "walk.2.dgut", "walk.1.log",
		"walk.2.log"}

	for _, suffix := range pathSuffixes {
		f, err := os.Create(filepath.Join(outputDir, suffix))
		if err != nil {
			t.Fatal(err)
		}

		f.Close()
	}
}

// writeToTestFiles writes an input of form (i+1)^2, where i is a for loop index
// number, to a test file. It returns a concatenated string of all the numbers
// it wrote to the files.
func writeToTestFiles(t *testing.T, testDir, testFileSuffix string) string {
	t.Helper()

	files, err := filepath.Glob(fmt.Sprintf("%s/*%s", testDir, testFileSuffix))
	if err != nil {
		t.Fatal(err)
	}

	var writtenInput string

	for i, file := range files {
		f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()

		_, err = f.WriteString(strconv.Itoa((i + 1) * (i + 1)))
		if err != nil {
			t.Fatal(err)
		}

		writtenInput += strconv.Itoa((i + 1) * (i + 1))
	}

	return writtenInput
}
