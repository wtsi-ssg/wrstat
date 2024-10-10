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
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/v5/dgut"
	"github.com/wtsi-ssg/wrstat/v5/fs"
	"github.com/wtsi-ssg/wrstat/v5/internal/encode"
	"github.com/wtsi-ssg/wrstat/v5/summary"
)

// TestDGUTFiles tests that the DGUT files merge properly to the output.
func TestDGUTFiles(t *testing.T) {
	Convey("Given dgut files and an output", t, func() {
		inputs, output, dir := buildDGUTFiles(t)

		Convey("You can merge the DGUT files and store to a db", func() {
			err := DgutFiles(inputs, output)
			So(err, ShouldBeNil)

			_, err = os.Stat(output)
			So(err, ShouldBeNil)

			Convey("and a query of the db data should be valid, and return the content of our DGUT testing files.", func() {
				db := dgut.NewDB(filepath.Join(dir, "combine.dgut.db"))
				So(db, ShouldNotBeNil)

				db.Close()

				err = db.Open()
				So(err, ShouldBeNil)

				ds, err := db.DirInfo("/", nil)
				So(err, ShouldBeNil)
				So(ds.Count, ShouldEqual, 3)
				So(ds.Size, ShouldEqual, 25)
				So(ds.Atime, ShouldEqual, time.Unix(1668768807, 0))
				So(ds.Mtime, ShouldEqual, time.Unix(1668768811, 0))
				So(ds.UIDs, ShouldResemble, []uint32{13912, 21574})
				So(ds.GIDs, ShouldResemble, []uint32{1313})
				So(ds.FTs, ShouldResemble, []summary.DirGUTAFileType{summary.DirGUTAFileType(0)})
				So(ds.SizeByAccessAge, ShouldEqual, [8]int64{25, 25, 25, 25, 25, 0, 0, 0})
				So(ds.SizeByModifyAge, ShouldEqual, [8]int64{25, 25, 25, 25, 25, 0, 0, 0})
			})
		})
	})
}

func TestOldFile(t *testing.T) {
	Convey("Given a dgut file describing old files", t, func() {
		dir := t.TempDir()

		now := time.Now().Unix()
		amtime1 := now - (summary.SecondsInAYear*5 + summary.SecondsInAMonth)
		amtime2 := now - (summary.SecondsInAYear*3 + summary.SecondsInAMonth)
		amtime3 := now - (summary.SecondsInAYear*7 + summary.SecondsInAMonth)

		output := filepath.Join(dir, "combine.dgut.db")

		f1 := createDGUTFile(t, dir, "file1", encode.Base64Encode("/")+fmt.Sprintf("\t1313\t22739\t0\t1\t1\t%d\t%d"+
			"\t1\t1\t1\t1\t1\t1\t1\t0\t1\t1\t1\t1\t1\t1\t1\t0\n", amtime1, amtime1))
		f2 := createDGUTFile(t, dir, "file2", encode.Base64Encode("/")+fmt.Sprintf("\t1313\t22739\t0\t1\t2\t%d\t%d"+
			"\t2\t2\t2\t2\t2\t2\t0\t0\t2\t2\t2\t2\t2\t2\t0\t0\n", amtime2, amtime2))
		f3 := createDGUTFile(t, dir, "file3", encode.Base64Encode("/")+fmt.Sprintf("\t1313\t22739\t0\t1\t3\t%d\t%d"+
			"\t3\t3\t3\t3\t3\t3\t3\t3\t3\t3\t3\t3\t3\t3\t3\t3\n", amtime3, amtime3))
		f4 := createDGUTFile(t, dir, "file4", encode.Base64Encode("/")+fmt.Sprintf("\t1313\t22739\t0\t1\t3\t%d\t%d"+
			"\t3\t3\t3\t3\t3\t3\t3\t3\t3\t3\t3\t3\t3\t3\t3\t3\n", amtime3, amtime3))
		f5 := createDGUTFile(t, dir, "file5", encode.Base64Encode("/")+fmt.Sprintf("\t1313\t22739\t0\t1\t4\t%d\t%d"+
			"\t4\t4\t4\t4\t4\t4\t4\t4\t4\t4\t4\t4\t4\t4\t0\t0\n", amtime3, amtime2))

		tfs := int64(1 + 2 + 3 + 3 + 4)
		expectedSizeByA := [8]int64{tfs, tfs, tfs, tfs, tfs, tfs, tfs - 2, tfs - 3}

		expectedSizeByM := [8]int64{tfs, tfs, tfs, tfs, tfs, tfs, tfs - 6, tfs - 7}

		Convey("if the directory is new then the mtime will match the directory", func() {
			d1 := createDGUTFile(t, dir, "dir", encode.Base64Encode("/")+fmt.Sprintf("\t1313\t22739\t15\t1\t4096\t%d\t%d"+
				"\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n", now, now))

			err := fs.RemoveAndCreateDir(output)
			So(err, ShouldBeNil)

			err = DgutFiles([]string{f1, f2, f3, f4, f5, d1}, output)
			So(err, ShouldBeNil)

			db := dgut.NewDB(output)
			So(db, ShouldNotBeNil)

			db.Close()

			err = db.Open()
			So(err, ShouldBeNil)

			ds, errd := db.DirInfo("/", nil)
			So(errd, ShouldBeNil)
			So(ds.Count, ShouldEqual, 6)
			So(ds.Size, ShouldEqual, tfs+4096)
			So(ds.Atime, ShouldEqual, time.Unix(amtime3, 0))
			So(ds.Mtime, ShouldEqual, time.Unix(now, 0))
			So(ds.SizeByAccessAge, ShouldEqual, expectedSizeByA)
			So(ds.SizeByModifyAge, ShouldEqual, expectedSizeByM)

			db.Close()
		})

		Convey("if the directory is older than the file, the mtime will match the file", func() {
			d1 := createDGUTFile(t, dir, "dir", encode.Base64Encode("/")+fmt.Sprintf("\t1313\t22739\t15\t1\t4096\t%d\t%d"+
				"\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n", amtime3-1, amtime3-1))

			err := fs.RemoveAndCreateDir(output)
			So(err, ShouldBeNil)

			err = DgutFiles([]string{f1, f2, f3, f4, f5, d1}, output)
			So(err, ShouldBeNil)

			db := dgut.NewDB(output)
			So(db, ShouldNotBeNil)

			db.Close()

			err = db.Open()
			So(err, ShouldBeNil)

			ds, errd := db.DirInfo("/", nil)
			So(errd, ShouldBeNil)
			So(ds.Count, ShouldEqual, 6)
			So(ds.Size, ShouldEqual, tfs+4096)
			So(ds.Atime, ShouldEqual, time.Unix(amtime3-1, 0))
			So(ds.Mtime, ShouldEqual, time.Unix(amtime2, 0))
			So(ds.SizeByAccessAge, ShouldEqual, expectedSizeByA)
			So(ds.SizeByModifyAge, ShouldEqual, expectedSizeByM)
		})
	})
}

// buildDGUTFiles builds the DGUT files for testing.
func buildDGUTFiles(t *testing.T) ([]string, string, string) {
	t.Helper()

	dir := t.TempDir()

	f1 := createDGUTFile(t, dir, "file1",
		buildDGUTContent("/long/file/path/used/for/testing", "1313", "13912", 0, 1, 0, 1668768807, 1668768808,
			[8]int64{0, 0, 0, 0, 0, 0, 0, 0}, [8]int64{0, 0, 0, 0, 0, 0, 0, 0}))
	f2 := createDGUTFile(t, dir, "file2",
		buildDGUTContent("/long/file/path/used/for/testing", "1313", "13912", 0, 1, 21, 1668768807, 1668768809,
			[8]int64{21, 21, 21, 21, 21, 0, 0, 0}, [8]int64{21, 21, 21, 21, 21, 0, 0, 0}))
	f3 := createDGUTFile(t, dir, "file3",
		buildDGUTContent("/long/file/path/used/for/testing", "1313", "21574", 0, 1, 4, 1668768810, 1668768811,
			[8]int64{4, 4, 4, 4, 4, 0, 0, 0}, [8]int64{4, 4, 4, 4, 4, 0, 0, 0}))

	output := filepath.Join(dir, "combine.dgut.db")

	err := fs.RemoveAndCreateDir(output)
	So(err, ShouldBeNil)

	return []string{f1, f2, f3}, output, dir
}

func createDGUTFile(t *testing.T, tempDir, fileName, content string) string {
	t.Helper()

	f, err := os.Create(filepath.Join(tempDir, fileName))
	So(err, ShouldBeNil)

	_, err = f.WriteString(content)
	So(err, ShouldBeNil)

	err = f.Close()
	So(err, ShouldBeNil)

	return f.Name()
}

// buildDGUTContent writes the top root from dir on line 1, and recursively
// appends the base of the root on line 2, 3, 4, etc. Appended to the path on
// each line, is the tab-separated data as follows: gid, uid, filetype,
// nestedFiles, fileSize, atime. For example,
// /	1313	13912	0	1	0	1668768807
// /lustre	1313	13912	0	1	0	1668768807
// /lustre/scratch123	1313	13912	0	1	0	1668768807.
func buildDGUTContent(directory, gid, uid string, filetype, nestedFiles,
	fileSize, oldestAtime, newestAtime int, sizeByAccessAge, sizeByModifyAge [8]int64) string {
	var DGUTContents string

	splitDir := recursivePath(directory)

	for _, split := range splitDir {
		DGUTContents += encode.Base64Encode(split) +
			fmt.Sprintf("\t%s\t%s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\n",
				gid, uid, filetype, nestedFiles, fileSize, oldestAtime, newestAtime,
				sizeByAccessAge[0], sizeByAccessAge[1], sizeByAccessAge[2], sizeByAccessAge[3],
				sizeByAccessAge[4], sizeByAccessAge[5], sizeByAccessAge[6], sizeByAccessAge[7],
				sizeByModifyAge[0], sizeByModifyAge[1], sizeByModifyAge[2], sizeByModifyAge[3],
				sizeByModifyAge[4], sizeByModifyAge[5], sizeByModifyAge[6], sizeByModifyAge[7])
	}

	return DGUTContents
}

// recursivePath takes a path, and into an array equal to the length of the
// path, recursively appends the path base, starting with the top dir. For
// example: /lustre/scratch123 would give,
// []string{"/", "/lustre", "/lustre/scratch123"}.
func recursivePath(path string) []string {
	count := strings.Count(path, "/")
	newPath := path

	var DGUTContents = make([]string, count+1)
	DGUTContents[count] = path

	for i := count - 1; i >= 0; i-- {
		DGUTContents[i] = filepath.Dir(newPath)
		newPath = filepath.Dir(newPath)
	}

	return DGUTContents
}
