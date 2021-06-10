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

package hashdir

import (
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestDirHasher(t *testing.T) {
	apath := "/a/file/path"

	Convey("The recommended number of levels is 4", t, func() {
		So(RecommendedLevels, ShouldEqual, 4)
	})

	Convey("Given a 4 level DirHasher", t, func() {
		h := New(4)
		So(h, ShouldNotBeNil)

		Convey("you can Hash() to get the correct sub dirs", func() {
			dirs := h.Hash(apath)
			So(len(dirs), ShouldEqual, 4)
			So(dirs, ShouldResemble, []string{"4", "a", "5", "601fe3ce8e586565fcbde2094cdaf"})

			Convey("and a hasher with 3 levels gives 3 sub dirs", func() {
				h = New(3)
				So(h, ShouldNotBeNil)

				dirs = h.Hash(apath)
				So(len(dirs), ShouldEqual, 3)
				So(dirs, ShouldResemble, []string{"4", "a", "5601fe3ce8e586565fcbde2094cdaf"})
			})
		})

		Convey("HashDir() returns the correct subdirs and leaf", func() {
			baseDir := "/tmp/foo"

			dirs, leaf, err := h.HashDir(baseDir, apath)
			So(err, ShouldBeNil)
			So(dirs, ShouldEqual, baseDir+"/4/a/5")
			So(leaf, ShouldEqual, "601fe3ce8e586565fcbde2094cdaf")
		})

		Convey("MkDirHashed() creates the correct subdirs and file", func() {
			baseDir, err := os.MkdirTemp("", "wrstat_dirhasher_test")
			So(err, ShouldBeNil)
			defer os.RemoveAll(baseDir)

			file, err := h.MkDirHashed(baseDir, apath)
			So(err, ShouldBeNil)
			defer file.Close()
			So(file.Name(), ShouldEqual, baseDir+"/4/a/5/601fe3ce8e586565fcbde2094cdaf")

			Convey("unless given an unwritable baseDir", func() {
				baseDir = "/~/"

				file, err = h.MkDirHashed(baseDir, apath)
				So(err, ShouldNotBeNil)
				So(file, ShouldBeNil)
			})
		})

		Convey("MkDirHashed() also works with relative baseDirs", func() {
			cwd, err := os.Getwd()
			So(err, ShouldBeNil)

			baseDir, err := os.MkdirTemp(cwd, "wrstat_dirhasher_test")
			So(err, ShouldBeNil)
			defer os.RemoveAll(baseDir)
			rel := filepath.Base(baseDir)

			file, err := h.MkDirHashed(rel, apath)
			So(err, ShouldBeNil)
			defer file.Close()
			So(file.Name(), ShouldEqual, baseDir+"/4/a/5/601fe3ce8e586565fcbde2094cdaf")
		})
	})
}
