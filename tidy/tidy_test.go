/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 *         Kyle Mace <km34@sanger.ac.uk>
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

package tidy

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestTidy(t *testing.T) {
	date := "20220829"

	Convey("Given existing source and dest dirs you can tidy the source", t, func() {
		tmpDir := t.TempDir()
		srcDir := filepath.Join(tmpDir, "src")
		destDir := filepath.Join(tmpDir, "dest")

		err := os.Mkdir(srcDir, modePermUser)
		So(err, ShouldBeNil)

		err = os.Mkdir(destDir, modePermUser)
		So(err, ShouldBeNil)

		// google "golang create file"
		emptyFile, err := os.Create("emptyFile.txt")

		So(err, ShouldBeNil)

		err = Up(srcDir, destDir, date)
		So(err, ShouldBeNil)

		// google "golang check if file exists"
		fileInfo, err := os.Stat("emptyFile.txt")
		So(err, ShouldBeNil)
		emptyFile.Close()
		fmt.Println(fileInfo)

		Convey("It also works if the dest dir doesn't exist", func() {
			err = os.RemoveAll(destDir)
			So(err, ShouldBeNil)

			err = Up(srcDir, destDir, date)
			So(err, ShouldBeNil)

			_, err = os.Stat(destDir)
			So(err, ShouldBeNil)
		})

		Convey("It doesn't work if source dir doesn't exist", func() {
			err = os.RemoveAll(srcDir)
			So(err, ShouldBeNil)

			err = Up(srcDir, destDir, date)
			So(err, ShouldNotBeNil)

			_, err = os.Stat(srcDir)
			So(err, ShouldNotBeNil)
		})

		Convey("It doesn't work if source or dest is an incorrect relative path", func() {
			relDir := filepath.Join(tmpDir, "rel")
			err = os.Mkdir(relDir, modePermUser)
			So(err, ShouldBeNil)

			err = os.Chdir(relDir)
			So(err, ShouldBeNil)

			err = Up("../src", "../dest", date)
			So(err, ShouldBeNil)

			err = os.RemoveAll(relDir)
			So(err, ShouldBeNil)

			err = Up("../src", "../dest", date)
			So(err, ShouldNotBeNil)
		})
	})
}

// src/sdf/sd/f/sdf/sd/stats.gz ...
// /sdfsdf//sdfsdf/output/..._
