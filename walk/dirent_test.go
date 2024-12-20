/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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

package walk

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDirent(t *testing.T) {
	Convey("Dirents of different types return correct Is*() results", t, func() {
		tdir := t.TempDir()
		reg := filepath.Join(tdir, "reg")
		f, err := os.Create(reg)
		So(err, ShouldBeNil)

		err = f.Close()
		So(err, ShouldBeNil)

		sym := filepath.Join(tdir, "sym")
		err = os.Symlink(reg, sym)
		So(err, ShouldBeNil)

		info, err := os.Lstat(tdir)
		So(err, ShouldBeNil)

		d := &Dirent{Type: info.Mode().Type()}
		So(d.IsDir(), ShouldBeTrue)
		So(d.IsRegular(), ShouldBeFalse)
		So(d.IsSymlink(), ShouldBeFalse)

		info, err = os.Lstat(reg)
		So(err, ShouldBeNil)

		d = &Dirent{Type: info.Mode().Type()}
		So(d.IsDir(), ShouldBeFalse)
		So(d.IsRegular(), ShouldBeTrue)
		So(d.IsSymlink(), ShouldBeFalse)

		info, err = os.Lstat(sym)
		So(err, ShouldBeNil)

		d = &Dirent{Type: info.Mode().Type()}
		So(d.IsDir(), ShouldBeFalse)
		So(d.IsRegular(), ShouldBeFalse)
		So(d.IsSymlink(), ShouldBeTrue)
	})

	Convey("You can make a fake Direct for directories", t, func() {
		d := newDirentForDirectoryPath("/a/dir")
		So(d.IsDir(), ShouldBeTrue)
		So(d.IsRegular(), ShouldBeFalse)
		So(d.IsSymlink(), ShouldBeFalse)
	})
}
