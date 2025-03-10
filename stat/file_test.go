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
	"strings"
	"syscall"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

type fsInfo struct {
	name string
	syscall.Stat_t
}

func (f *fsInfo) IsDir() bool {
	return f.Stat_t.Mode&uint32(fs.ModeDir) > 0
}

func (f *fsInfo) ModTime() time.Time {
	return time.Unix(f.Mtim.Sec, f.Mtim.Nsec)
}

func (f *fsInfo) Mode() fs.FileMode {
	return fs.FileMode(f.Stat_t.Mode)
}

func (f *fsInfo) Name() string {
	return f.name
}

func (f *fsInfo) Size() int64 {
	return f.Stat_t.Size
}

func (f *fsInfo) Sys() any {
	return &f.Stat_t
}

func TestStatFile(t *testing.T) {
	Convey("FileStats can correct its size", t, func() {
		fstat := &fsInfo{
			name: "something.txt",
			Stat_t: syscall.Stat_t{
				Size:   54321,
				Blocks: 12,
			},
		}

		So(File("/some/path/something", fstat, false), ShouldResemble, FileStats{
			Path:  "/some/path/something",
			Size:  54321,
			ASize: 54321,
			Type:  "f",
		})

		So(File("/some/path/something", fstat, true), ShouldResemble, FileStats{
			Path:  "/some/path/something",
			Size:  512 * 12,
			ASize: 54321,
			Type:  "f",
		})
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

				testFileStats(link, int64(len(reg)), "l")
			})
		})
	})
}

func testFileStats(path string, size int64, filetype string) {
	info, err := os.Lstat(path)
	So(err, ShouldBeNil)

	stats := File("/abs/path/to/file", info, false)
	So(stats, ShouldNotBeNil)
	So(len(stats.Path), ShouldBeGreaterThan, 0)
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

	var sb strings.Builder

	n, err := stats.WriteTo(&sb)
	So(err, ShouldBeNil)
	So(n, ShouldNotBeZeroValue)

	So(sb.String(), ShouldEqual, fmt.Sprintf(
		"%s\t%d\t%d\t%d\t%d\t%d\t%d\t%s\t%d\t%d\t%d\t%d\n",
		strconv.Quote("/abs/path/to/file"), size, stat.Uid, stat.Gid,
		stat.Atim.Sec, stat.Mtim.Sec, stat.Ctim.Sec,
		filetype, stat.Ino, stat.Nlink, stat.Dev, size))
}
