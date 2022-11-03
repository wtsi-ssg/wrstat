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
	cmd "github.com/wtsi-ssg/wrstat/cmd"
)

func TestCombine(t *testing.T) { //nolint:gocognit
	Convey("The function combines the files", t, func() {
		testDir := t.TempDir()

		buildOutputDir(t, testDir)

		Convey("And all functionality fails if there are no stat, group, user group, dgut and log files.", func() {
			buildOutputDir(t, testDir)
			logFiles, err := filepath.Glob(fmt.Sprintf("%s/*%s", testDir, ".log"))
			So(err, ShouldBeNil)
			for _, file := range logFiles {
				os.Remove(file)
			}

			err = MergeAndOptionallyCompressFiles(testDir, ".log", "combine.log.gz", cmd.MergeLogStreamToCompressedFile)
			So(err, ShouldNotBeNil)

			buildOutputDir(t, testDir)
			byusergroupFiles, err := filepath.Glob(fmt.Sprintf("%s/*%s", testDir, ".byusergroup"))
			So(err, ShouldBeNil)
			for _, file := range byusergroupFiles {
				os.Remove(file)
			}

			err = MergeAndOptionallyCompressFiles(testDir, ".byusergroup", "combine.byusergroup.gz",
				cmd.MergeUserGroupStreamToCompressedFile)
			So(err, ShouldNotBeNil)

			buildOutputDir(t, testDir)
			bygroupFiles, err := filepath.Glob(fmt.Sprintf("%s/*%s", testDir, ".bygroup"))
			So(err, ShouldBeNil)
			for _, file := range bygroupFiles {
				os.Remove(file)
			}

			err = MergeAndOptionallyCompressFiles(testDir, ".bygroup", "combine.bygroup", cmd.MergeGroupStreamToFile)
			So(err, ShouldNotBeNil)

			buildOutputDir(t, testDir)
			statsFiles, err := filepath.Glob(fmt.Sprintf("%s/*%s", testDir, ".stats"))
			So(err, ShouldBeNil)
			for _, file := range statsFiles {
				os.Remove(file)
			}

			err = CompressAndConcatenate(testDir, ".stats", "combine.stats.gz")
			So(err, ShouldNotBeNil)

			buildOutputDir(t, testDir)
			dgutFiles, err := filepath.Glob(fmt.Sprintf("%s/*%s", testDir, ".dgut"))
			So(err, ShouldBeNil)
			for _, file := range dgutFiles {
				os.Remove(file)
			}

			err = MergeDGUTFilesToDB(testDir)
			So(err, ShouldNotBeNil)
		})
		Convey("And all functionality fails if the source dir does not exist.", func() {
			err := os.RemoveAll(testDir)
			So(err, ShouldBeNil)

			_, err = os.Stat(testDir)
			So(err, ShouldNotBeNil)

			err1 := MergeAndOptionallyCompressFiles(testDir, ".log", "combine.log.gz", cmd.MergeLogStreamToCompressedFile)
			err2 := MergeAndOptionallyCompressFiles(testDir, ".byusergroup", "combine.byusergroup.gz",
				cmd.MergeUserGroupStreamToCompressedFile)
			err3 := MergeAndOptionallyCompressFiles(testDir, ".bygroup", "combine.bygroup", cmd.MergeGroupStreamToFile)
			err4 := CompressAndConcatenate(testDir, ".stats", "combine.stats.gz")
			err5 := MergeDGUTFilesToDB(testDir)

			So(err1, ShouldNotBeNil)
			So(err2, ShouldNotBeNil)
			So(err3, ShouldNotBeNil)
			So(err4, ShouldNotBeNil)
			So(err5, ShouldNotBeNil)
		})
		Convey("And all functionality fails if an incorrect relative path is supplied", func() {
			relativeDir := filepath.Join(testDir, "rel")
			err := os.MkdirAll(relativeDir, 448)
			So(err, ShouldBeNil)

			err = os.Chdir(relativeDir)
			So(err, ShouldBeNil)

			err = os.RemoveAll(relativeDir)
			So(err, ShouldBeNil)

			relativeDir += "../"

			err1 := MergeAndOptionallyCompressFiles(relativeDir, ".log", "combine.log.gz", cmd.MergeLogStreamToCompressedFile)
			err2 := MergeAndOptionallyCompressFiles(relativeDir, ".byusergroup", "combine.byusergroup.gz",
				cmd.MergeUserGroupStreamToCompressedFile)
			err3 := MergeAndOptionallyCompressFiles(relativeDir, ".bygroup", "combine.bygroup.gz", cmd.MergeGroupStreamToFile)
			err4 := CompressAndConcatenate(relativeDir, ".stats", "combine.stats.gz")
			err5 := MergeDGUTFilesToDB(relativeDir)

			So(err1, ShouldNotBeNil)
			So(err2, ShouldNotBeNil)
			So(err3, ShouldNotBeNil)
			So(err4, ShouldNotBeNil)
			So(err5, ShouldNotBeNil)
		})
		Convey(`And there exist the files combine.stats.gz, combine.byusergroup.gz, combine.log.gz, combine.bygroup, 
		combine.dgut.db at the root of output dir`, func() {
			err1 := MergeAndOptionallyCompressFiles(testDir, ".log", "combine.log.gz", cmd.MergeLogStreamToCompressedFile)
			err2 := MergeAndOptionallyCompressFiles(testDir, ".byusergroup", "combine.byusergroup.gz",
				cmd.MergeUserGroupStreamToCompressedFile)
			err3 := MergeAndOptionallyCompressFiles(testDir, ".bygroup", "combine.bygroup", cmd.MergeGroupStreamToFile)
			err4 := CompressAndConcatenate(testDir, ".stats", "combine.stats.gz")
			err5 := MergeDGUTFilesToDB(testDir)

			So(err1, ShouldBeNil)
			So(err2, ShouldBeNil)
			So(err3, ShouldBeNil)
			So(err4, ShouldBeNil)
			So(err5, ShouldBeNil)

			expectedFiles := [5]string{"combine.stats.gz", "combine.byusergroup.gz", "combine.log.gz",
				"combine.bygroup", "combine.dgut.db"}

			for _, file := range expectedFiles {
				expectedFile := filepath.Join(testDir, file)
				_, err := os.Stat(expectedFile)
				So(err, ShouldBeNil)
			}
		})
		Convey("And the files have been properly compressed", func() {
			expectedCompressedFiles := [3]string{"combine.stats.gz", "combine.byusergroup.gz", "combine.log.gz"}

			err1 := MergeAndOptionallyCompressFiles(testDir, ".log", "combine.log.gz", cmd.MergeLogStreamToCompressedFile)
			err2 := MergeAndOptionallyCompressFiles(testDir, ".byusergroup", "combine.byusergroup.gz",
				cmd.MergeUserGroupStreamToCompressedFile)
			err3 := CompressAndConcatenate(testDir, ".stats", "combine.stats.gz")

			So(err1, ShouldBeNil)
			So(err2, ShouldBeNil)
			So(err3, ShouldBeNil)

			for _, file := range expectedCompressedFiles {
				f, err := os.ReadFile(filepath.Join(testDir, file))
				So(err, ShouldBeNil)

				expectedFileType := "application/x-gzip"
				fileType := http.DetectContentType(f)
				So(fileType, ShouldEqual, expectedFileType)
			}
		})
		Convey(`And combine.stats.gz, combine.log.gz, combine.byusergroup.gz contain the merging or concatenation of 
		their corresponding input files.`, func() {
			statsOutputPath := filepath.Join(testDir, "combine.stats.gz")
			expectedStatsFileContents := writeToTestFiles(t, testDir, ".stats")

			logOutputPath := filepath.Join(testDir, "combine.log.gz")
			expectedLogFileContents := writeToTestFiles(t, testDir, ".log")

			byusergroupOutputPath := filepath.Join(testDir, "combine.byusergroup.gz")
			expectedByusergroupFileContents := writeToTestFiles(t, testDir, ".byusergroup")

			err1 := CompressAndConcatenate(testDir, ".stats", "combine.stats.gz")
			err2 := MergeAndOptionallyCompressFiles(testDir, ".log", "combine.log.gz", cmd.MergeLogStreamToCompressedFile)
			err3 := MergeAndOptionallyCompressFiles(testDir, ".byusergroup", "combine.byusergroup.gz",
				cmd.MergeUserGroupStreamToCompressedFile)

			So(err1, ShouldBeNil)
			So(err2, ShouldBeNil)
			So(err3, ShouldBeNil)

			statsFile, err := os.Open(statsOutputPath)
			So(err, ShouldBeNil)
			logFile, err := os.Open(logOutputPath)
			So(err, ShouldBeNil)
			byusergroupFile, err := os.Open(byusergroupOutputPath)
			So(err, ShouldBeNil)

			statsFileReader, err := gzip.NewReader(statsFile)
			So(err, ShouldBeNil)
			logFileReader, err := gzip.NewReader(logFile)
			So(err, ShouldBeNil)
			byusergroupFileReader, err := gzip.NewReader(byusergroupFile)
			So(err, ShouldBeNil)

			defer statsFileReader.Close()
			defer logFileReader.Close()
			defer byusergroupFileReader.Close()

			statsFileScanner := bufio.NewScanner(statsFileReader)
			logFileScanner := bufio.NewScanner(logFileReader)
			byusergroupFileScanner := bufio.NewScanner(byusergroupFileReader)

			var statsFileContents string
			for statsFileScanner.Scan() {
				statsFileContents += statsFileScanner.Text()
			}
			var logFileContents string
			for logFileScanner.Scan() {
				logFileContents += logFileScanner.Text()
			}
			var byusergroupFileContents string
			for byusergroupFileScanner.Scan() {
				byusergroupFileContents += byusergroupFileScanner.Text()
			}

			So(statsFileContents, ShouldEqual, expectedStatsFileContents)
			So(logFileContents, ShouldEqual, expectedLogFileContents)
			So(byusergroupFileContents, ShouldEqual, expectedByusergroupFileContents)
		})
		Convey("And combine.bygroup contains the merged contents of the .bygroup files.", func() {
			expectedOutputPath := filepath.Join(testDir, "combine.bygroup")
			expectedBygroupFileContents := writeToTestFiles(t, testDir, ".bygroup")

			err := MergeAndOptionallyCompressFiles(testDir, ".bygroup", "combine.bygroup", cmd.MergeGroupStreamToFile)
			So(err, ShouldBeNil)

			bygroupFile, err := os.ReadFile(expectedOutputPath)
			So(err, ShouldBeNil)

			bygroupFileContents := strings.ReplaceAll(string(bygroupFile), "\n", "")

			So(bygroupFileContents, ShouldEqual, expectedBygroupFileContents)
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
