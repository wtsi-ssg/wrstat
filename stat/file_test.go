/*******************************************************************************
 * Copyright (c) 2021 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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

package stat

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestStatFile(t *testing.T) {
	Convey("FileStats can correct its size", t, func() {
		fstat := &FileStats{
			Size: 10,
		}

		stat := &syscall.Stat_t{
			Blocks:  1,
			Blksize: 1024,
		}
		fstat.correctSize(stat)
		So(fstat.Size, ShouldEqual, 10)

		fstat.Size = 1025
		fstat.correctSize(stat)
		So(fstat.Size, ShouldEqual, 512)

		stat.Blocks = 0
		fstat.correctSize(stat)
		So(fstat.Size, ShouldEqual, 0)
	})

	Convey("modeToType() works correctly", t, func() {
		So(modeToType(fs.FileMode(0)), ShouldEqual, "f")
		So(modeToType(fs.ModeDir), ShouldEqual, "d")
		So(modeToType(fs.ModeSymlink), ShouldEqual, "l")
		So(modeToType(fs.ModeSocket), ShouldEqual, "s")
		So(modeToType(fs.ModeDevice), ShouldEqual, "b")
		So(modeToType(fs.ModeCharDevice), ShouldEqual, "c")
		So(modeToType(fs.ModeNamedPipe), ShouldEqual, "F")
		So(modeToType(fs.ModeIrregular), ShouldEqual, "X")
	})

	Convey("File() returns the correct interpretation of FileInfo", t, func() {
		dir, err := os.MkdirTemp("", "wrstat_statfile_test")
		So(err, ShouldBeNil)
		defer os.RemoveAll(dir)

		Convey("for a regular file", func() {
			reg := filepath.Join(dir, "reg")
			file, err := os.Create(reg)
			So(err, ShouldBeNil)

			n, err := file.WriteString("1")
			So(err, ShouldBeNil)
			So(n, ShouldEqual, 1)

			err = file.Close()
			So(err, ShouldBeNil)

			testFileStats(reg, 1, "f")

			Convey("and a symlink", func() {
				link := filepath.Join(dir, "link")
				err := os.Symlink(reg, link)
				So(err, ShouldBeNil)

				testFileStats(link, 0, "l")
			})
		})
	})
}

func testFileStats(path string, size int64, filetype string) {
	info, err := os.Lstat(path)
	So(err, ShouldBeNil)

	stats := File("/abs/path/to/file", info)
	So(stats, ShouldNotBeNil)
	So(len(stats.QuotedPath), ShouldBeGreaterThan, 0)
	So(stats.Size, ShouldEqual, size)

	stat, ok := info.Sys().(*syscall.Stat_t)
	So(ok, ShouldBeTrue)
	So(stats.UID, ShouldEqual, stat.Uid)
	So(stats.GID, ShouldEqual, stat.Gid)
	So(stats.Atim, ShouldEqual, stat.Atim.Sec)
	So(stats.Mtim, ShouldEqual, stat.Mtim.Sec)
	So(stats.Ctim, ShouldEqual, stat.Ctim.Sec)
	So(stats.Type, ShouldEqual, filetype)
	So(stats.Ino, ShouldEqual, stat.Ino)
	So(stats.Nlink, ShouldEqual, stat.Nlink)
	So(stats.Dev, ShouldEqual, stat.Dev)

	So(stats.ToString(), ShouldEqual, fmt.Sprintf(
		"%s\t%d\t%d\t%d\t%d\t%d\t%d\t%s\t%d\t%d\t%d\n",
		strconv.Quote("/abs/path/to/file"), size, stat.Uid, stat.Gid,
		stat.Atim.Sec, stat.Mtim.Sec, stat.Ctim.Sec,
		filetype, stat.Ino, stat.Nlink, stat.Dev))
}
