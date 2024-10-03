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
		inputs, output, outputPath, dir := buildDGUTFiles(t)

		Convey("You can merge the DGUT files and store to a db", func() {
			err := DgutFiles(inputs, output)
			So(err, ShouldBeNil)

			_, err = os.Stat(outputPath)
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
				So(ds.UIDs, ShouldResemble, []uint32{13912, 13913, 21574})
				So(ds.GIDs, ShouldResemble, []uint32{1313})
				So(ds.FTs, ShouldResemble, []summary.DirGUTFileType{summary.DirGUTFileType(0)})
				So(ds.SizeByAccessAge, ShouldEqual, [8]int64{25, 25, 25, 25, 25, 0, 0, 0})
				So(ds.SizeByModifyAge, ShouldEqual, [8]int64{25, 25, 25, 25, 25, 0, 0, 0})
			})
		})
	})
}

// buildDGUTFiles builds the DGUT files for testing.
func buildDGUTFiles(t *testing.T) ([]string, string, string, string) {
	t.Helper()

	dir := t.TempDir()

	f1, err := os.Create(filepath.Join(dir, "file1"))
	So(err, ShouldBeNil)
	f2, err := os.Create(filepath.Join(dir, "file2"))
	So(err, ShouldBeNil)
	f3, err := os.Create(filepath.Join(dir, "file3"))
	So(err, ShouldBeNil)

	file1Content := buildDGUTContent("/long/file/path/used/for/testing", "1313", "13912", 0, 1, 0, 1668768807, 1668768808,
		[8]int64{0, 0, 0, 0, 0, 0, 0, 0}, [8]int64{0, 0, 0, 0, 0, 0, 0, 0})
	file2Content := buildDGUTContent("/long/file/path/used/for/testing", "1313", "13913", 0, 1, 21, 1668768807, 1668768809,
		[8]int64{21, 21, 21, 21, 21, 0, 0, 0}, [8]int64{21, 21, 21, 21, 21, 0, 0, 0})
	file3Content := buildDGUTContent("/long/file/path/used/for/testing", "1313", "21574", 0, 1, 4, 1668768810, 1668768811,
		[8]int64{4, 4, 4, 4, 4, 0, 0, 0}, [8]int64{4, 4, 4, 4, 4, 0, 0, 0})

	_, err = f1.WriteString(file1Content)
	So(err, ShouldBeNil)
	_, err = f2.WriteString(file2Content)
	So(err, ShouldBeNil)
	_, err = f3.WriteString(file3Content)
	So(err, ShouldBeNil)

	outputPath := filepath.Join(dir, "combine.dgut.db")
	output := filepath.Join(dir, "combine.dgut.db")

	err = fs.RemoveAndCreateDir(output)
	So(err, ShouldBeNil)

	return []string{f1.Name(), f2.Name(), f3.Name()}, output, outputPath, dir
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
