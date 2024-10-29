/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * 		   Kyle Mace  <km34@sanger.ac.uk>
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

package combine

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/v5/dguta"
	"github.com/wtsi-ssg/wrstat/v5/fs"
	"github.com/wtsi-ssg/wrstat/v5/summary"
)

// TestDGUTAFiles tests that the DGUTA files merge properly to the output.
func TestDGUTAFiles(t *testing.T) {
	Convey("Given dguta files and an output", t, func() {
		curUnixTime := time.Now().Unix()
		inputs, output, dir := buildDGUTAFiles(t, curUnixTime)

		Convey("You can merge the DGUTA files and store to a db", func() {
			err := DgutaFiles(inputs, output)
			So(err, ShouldBeNil)

			_, err = os.Stat(output)
			So(err, ShouldBeNil)

			Convey("and a query of the db data should be valid, and return the content of our DGUTA testing files.", func() {
				db := dguta.NewDB(filepath.Join(dir, "combine.dguta.db"))
				So(db, ShouldNotBeNil)

				db.Close()

				err = db.Open()
				So(err, ShouldBeNil)

				AMTime1 := curUnixTime - (summary.SecondsInAYear*5 + summary.SecondsInAMonth)

				ds, err := db.DirInfo("/", &dguta.Filter{Age: summary.DGUTAgeAll})
				So(err, ShouldBeNil)
				So(ds.Count, ShouldEqual, 3)
				So(ds.Size, ShouldEqual, 25)
				So(ds.Atime, ShouldEqual, time.Unix(AMTime1, 0))
				So(ds.Mtime, ShouldEqual, time.Unix(curUnixTime, 0))
				So(ds.UIDs, ShouldResemble, []uint32{13912, 21574})
				So(ds.GIDs, ShouldResemble, []uint32{1313})
				So(ds.FTs, ShouldResemble, []summary.DirGUTAFileType{summary.DirGUTAFileType(0)})
				So(ds.Age, ShouldEqual, summary.DGUTAgeAll)

				ds, err = db.DirInfo("/", &dguta.Filter{Age: summary.DGUTAgeA1M})
				So(err, ShouldBeNil)
				So(ds.Count, ShouldEqual, 2)
				So(ds.Size, ShouldEqual, 25)
				So(ds.Age, ShouldEqual, summary.DGUTAgeA1M)

				ds, err = db.DirInfo("/", &dguta.Filter{Age: summary.DGUTAgeA2Y})
				So(err, ShouldBeNil)
				So(ds.Count, ShouldEqual, 1)
				So(ds.Size, ShouldEqual, 4)
				So(ds.Age, ShouldEqual, summary.DGUTAgeA2Y)

				ds, err = db.DirInfo("/", &dguta.Filter{Age: summary.DGUTAgeA7Y})
				So(err, ShouldBeNil)
				So(ds, ShouldBeNil)
			})
		})
	})
}

func TestOldFile(t *testing.T) {
	Convey("Given a dguta file describing old files", t, func() {
		dir := t.TempDir()

		curUnixTime := time.Now().Unix()
		amtime1 := curUnixTime - (summary.SecondsInAYear*5 + summary.SecondsInAMonth)
		amtime2 := curUnixTime - (summary.SecondsInAYear*3 + summary.SecondsInAMonth)
		amtime3 := curUnixTime - (summary.SecondsInAYear*7 + summary.SecondsInAMonth)

		output := filepath.Join(dir, "combine.dguta.db")

		f1 := createDGUTAFile(t, dir, "file1",
			buildDGUTAContent("/", "1313", "22739", 0, 1, 1, amtime1, amtime1, curUnixTime))
		f2 := createDGUTAFile(t, dir, "file2",
			buildDGUTAContent("/", "1313", "22739", 0, 1, 2, amtime2, amtime2, curUnixTime))
		f3 := createDGUTAFile(t, dir, "file3",
			buildDGUTAContent("/", "1313", "22739", 0, 1, 3, amtime3, amtime3, curUnixTime))
		f4 := createDGUTAFile(t, dir, "file4",
			buildDGUTAContent("/", "1313", "22739", 0, 1, 3, amtime3, amtime3, curUnixTime))
		f5 := createDGUTAFile(t, dir, "file5",
			buildDGUTAContent("/", "1313", "22739", 0, 1, 4, amtime3, amtime2, curUnixTime))

		tfs := int64(1 + 2 + 3 + 3 + 4)
		expectedCount := 6

		Convey("if the directory is new then the mtime will match the directory", func() {
			d1 := createDGUTAFile(t, dir, "dir",
				buildDGUTAContent("/", "1313", "22739", 15, 1, 4096, curUnixTime, curUnixTime, curUnixTime))

			err := fs.RemoveAndCreateDir(output)
			So(err, ShouldBeNil)

			err = DgutaFiles([]string{f1, f2, f3, f4, f5, d1}, output)
			So(err, ShouldBeNil)

			db := dguta.NewDB(output)
			So(db, ShouldNotBeNil)

			db.Close()

			err = db.Open()
			So(err, ShouldBeNil)

			ds, errd := db.DirInfo("/", &dguta.Filter{Age: summary.DGUTAgeAll})
			So(errd, ShouldBeNil)
			So(ds.Count, ShouldEqual, expectedCount)
			So(ds.Size, ShouldEqual, tfs+4096)
			So(ds.Atime, ShouldEqual, time.Unix(amtime3, 0))
			So(ds.Mtime, ShouldEqual, time.Unix(curUnixTime, 0))

			db.Close()
		})

		Convey("if the directory is older than the file, the mtime will match the file", func() {
			d1 := createDGUTAFile(t, dir, "dir",
				buildDGUTAContent("/", "1313", "22739", 15, 1, 4096, amtime3-1, amtime3-1, curUnixTime))

			err := fs.RemoveAndCreateDir(output)
			So(err, ShouldBeNil)

			err = DgutaFiles([]string{f1, f2, f3, f4, f5, d1}, output)
			So(err, ShouldBeNil)

			db := dguta.NewDB(output)
			So(db, ShouldNotBeNil)

			db.Close()

			err = db.Open()
			So(err, ShouldBeNil)

			ds, errd := db.DirInfo("/", &dguta.Filter{Age: summary.DGUTAgeAll})
			So(errd, ShouldBeNil)
			So(ds.Mtime, ShouldEqual, time.Unix(amtime2, 0))

			Convey("and the DirGUTAges are set as expected", func() {
				expectedSizes := [17]int64{
					tfs, tfs, tfs, tfs, tfs, tfs, tfs, tfs - 2,
					tfs - 3, tfs, tfs, tfs, tfs, tfs, tfs, tfs - 6, tfs - 7,
				}

				expectedCounts := [17]int{
					expectedCount, expectedCount, expectedCount,
					expectedCount, expectedCount, expectedCount, expectedCount,
					expectedCount - 1, expectedCount - 2, expectedCount, expectedCount,
					expectedCount, expectedCount, expectedCount, expectedCount,
					expectedCount - 2, expectedCount - 3,
				}

				expectedAtime := amtime3 - 1

				expectedMtimes := [17]int64{
					amtime2, amtime2, amtime2, amtime2, amtime2, amtime2, amtime2, amtime2, amtime2,
					amtime2, amtime2, amtime2, amtime2, amtime2, amtime2, amtime1, amtime3,
				}

				for i, age := range summary.DirGUTAges {
					ds, errd := db.DirInfo("/", &dguta.Filter{Age: age})
					So(errd, ShouldBeNil)
					So(ds.Count, ShouldEqual, expectedCounts[i])
					So(ds.Size, ShouldEqual, expectedSizes[i]+4096)
					So(ds.Atime, ShouldEqual, time.Unix(expectedAtime, 0))
					So(ds.Mtime, ShouldEqual, time.Unix(expectedMtimes[i], 0))
				}
			})
		})
	})
}

// buildDGUTAFiles builds the DGUTA files for testing.
func buildDGUTAFiles(t *testing.T, curUnixTime int64) ([]string, string, string) {
	t.Helper()

	dir := t.TempDir()

	f2AMTime := curUnixTime - (summary.SecondsInAYear + summary.SecondsInAMonth)
	f3AMTime := curUnixTime - (summary.SecondsInAYear*5 + summary.SecondsInAMonth)

	f1 := createDGUTAFile(t, dir, "file1",
		buildDGUTAContent("/long/file/path/used/for/testing", "1313", "13912",
			0, 1, 0, curUnixTime, curUnixTime, curUnixTime))
	f2 := createDGUTAFile(t, dir, "file2",
		buildDGUTAContent("/long/file/path/used/for/testing", "1313", "13912",
			0, 1, 21, f2AMTime, f2AMTime, curUnixTime))
	f3 := createDGUTAFile(t, dir, "file3",
		buildDGUTAContent("/long/file/path/used/for/testing", "1313", "21574",
			0, 1, 4, f3AMTime, f3AMTime, curUnixTime))

	output := filepath.Join(dir, "combine.dguta.db")

	err := fs.RemoveAndCreateDir(output)
	So(err, ShouldBeNil)

	return []string{f1, f2, f3}, output, dir
}

func createDGUTAFile(t *testing.T, tempDir, fileName, content string) string {
	t.Helper()

	f, err := os.Create(filepath.Join(tempDir, fileName))
	So(err, ShouldBeNil)

	_, err = f.WriteString(content)
	So(err, ShouldBeNil)

	err = f.Close()
	So(err, ShouldBeNil)

	return f.Name()
}

// buildDGUTAContent writes the top root from dir on line 1, and recursively
// appends the base of the root on line 2, 3, 4, etc. It also writes a line for
// each DirGUTAge that the given data is valid for. Appended to the path on each
// line, is the tab-separated data as follows: gid, uid, filetype, age,
// nestedFiles, fileSize, atime. For example, /    1313    13912   0   0   1   0
// 1668768807 /lustre  1313    13912   0   0   1   0   1668768807
// /lustre/scratch123   1313    13912   0   0   1   0   1668768807.
func buildDGUTAContent(directory, gid, uid string, filetype, nestedFiles, //nolint:unparam
	fileSize, oldestAtime, newestAtime, refTime int64,
) string {
	var dgutaContents string

	splitDir := recursivePath(directory)

	for _, split := range splitDir {
		for _, age := range summary.DirGUTAges {
			guta := fmt.Sprintf("\t%s\t%s\t%d\t%d", gid, uid, filetype, age)

			if !summary.FitsAgeInterval(guta, oldestAtime, newestAtime, refTime) {
				continue
			}

			dgutaContents += strconv.Quote(split) + guta +
				fmt.Sprintf("\t%d\t%d\t%d\t%d\n",
					nestedFiles, fileSize, oldestAtime, newestAtime)
		}
	}

	return dgutaContents
}

// recursivePath takes a path, and into an array equal to the length of the
// path, recursively appends the path base, starting with the top dir. For
// example: /lustre/scratch123 would give,
// []string{"/", "/lustre", "/lustre/scratch123"}.
// If the given path is "/" it just returns "/".
func recursivePath(path string) []string {
	if path == "/" {
		return []string{"/"}
	}

	count := strings.Count(path, "/")
	newPath := path

	dgutaContents := make([]string, count+1)
	dgutaContents[count] = path

	for i := count - 1; i >= 0; i-- {
		dgutaContents[i] = filepath.Dir(newPath)
		newPath = filepath.Dir(newPath)
	}

	return dgutaContents
}
