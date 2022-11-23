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

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/v3/fs"
)

// TestDGUTFiles tests that the DGUT files merge properly to the output.
func TestDGUTFiles(t *testing.T) {
	Convey("Given log files and an output", t, func() {
		inputs, output, outputPath := buildDGUTFiles(t)

		Convey("You can merge the DGUT files and store to a db", func() {
			err := DgutFiles(inputs, output)
			So(err, ShouldBeNil)

			_, err = os.Stat(outputPath)
			So(err, ShouldBeNil)
		})
		/*
			ah, err := dgutDBCombinePaths(thisDir)
			So(err, ShouldBeNil)

			tree, err := dgut.NewTree(ah...)
			So(err, ShouldBeNil)
			So(tree, ShouldEqual, "")*/
	})
}

// buildDGUTFiles builds the DGUT files for testing.
func buildDGUTFiles(t *testing.T) ([]string, string, string) {
	t.Helper()

	dir := t.TempDir()

	f1, err := os.Create(filepath.Join(dir, "file1"))
	So(err, ShouldBeNil)
	f2, err := os.Create(filepath.Join(dir, "file2"))
	So(err, ShouldBeNil)
	f3, err := os.Create(filepath.Join(dir, "file3"))
	So(err, ShouldBeNil)

	file1Content := buildDGUTContent("/lustre/scratch123/hgi/teams/hgi/mercury/km34_wrstat/src",
		"1313", "13912", 0, 1, 0, 1668768807)
	file2Content := buildDGUTContent("/lustre/scratch123/hgi/teams/hgi/mercury/km34_wrstat/src",
		"1313", "13912", 0, 1, 21, 1668768807)
	file3Content := buildDGUTContent("/lustre/scratch123/hgi/teams/hgi/mercury/km34_wrstat/src",
		"1313", "21574", 0, 1, 0, 1668768810)

	_, err = f1.WriteString(file1Content)
	So(err, ShouldBeNil)
	_, err = f2.WriteString(file2Content)
	So(err, ShouldBeNil)
	_, err = f3.WriteString(file3Content)
	So(err, ShouldBeNil)

	outputPath := filepath.Join(dir, "combine.dgut.db")
	output, err := fs.RemoveAndCreateDir(filepath.Join(dir, "combine.dgut.db"))
	So(err, ShouldBeNil)

	return []string{f1.Name(), f2.Name(), f3.Name()}, output, outputPath
}

// buildDGUTContent writes the top root from dir on line 1, and recursively
// appends the dir of the root on line 2, 3, 4, etc. Appended to the path on
// each line, is the tab-separated data as follows: gid, uid, filetype,
// nestedFiles, fileSize, atime. For example,
// /	1313	13912	0	1	0	1668768807
// /lustre	1313	13912	0	1	0	1668768807
// /lustre/scratch123	1313	13912	0	1	0	1668768807.
func buildDGUTContent(directory, gid, uid string, filetype, nestedFiles, fileSize, atime int) string {
	var DGUTContents string

	splitDir := recursivePath(directory)

	for _, split := range splitDir {
		DGUTContents += split + fmt.Sprintf("\t%s\t%s\t%d\t%d\t%d\t%d\n", gid, uid, filetype, nestedFiles, fileSize, atime)
	}

	return DGUTContents
}

// recursivePath takes a path, and into an array equal to the length of the
// path, recursively writes the path from the top root to the full path. For
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