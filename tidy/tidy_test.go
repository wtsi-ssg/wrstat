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

Given a srcDir of multi and a destDir of multi/final, and told to work on
go and perl folders, tidy produces:

multi
multi/final
multi/final/20220916_go.cci4au7nu1ibc2ta5j80.cci4au7nu1ibc2ta5j7g.stats.gz
multi/final/20220916_perl.cci4au7nu1ibc2ta5j8g.cci4au7nu1ibc2ta5j7g.stats.gz
multi/final/20220916_go.cci4au7nu1ibc2ta5j80.cci4au7nu1ibc2ta5j7g.byusergroup.gz
multi/final/20220916_perl.cci4au7nu1ibc2ta5j8g.cci4au7nu1ibc2ta5j7g.byusergroup.gz
multi/final/20220916_go.cci4au7nu1ibc2ta5j80.cci4au7nu1ibc2ta5j7g.bygroup
multi/final/20220916_perl.cci4au7nu1ibc2ta5j8g.cci4au7nu1ibc2ta5j7g.bygroup
multi/final/20220916_go.cci4au7nu1ibc2ta5j80.cci4au7nu1ibc2ta5j7g.logs.gz
multi/final/20220916_perl.cci4au7nu1ibc2ta5j8g.cci4au7nu1ibc2ta5j7g.logs.gz
multi/final/20220916_cci4au7nu1ibc2ta5j7g.basedirs
multi/final/20220916_cci4au7nu1ibc2ta5j7g.dgut.dbs
multi/final/20220916_cci4au7nu1ibc2ta5j7g.dgut.dbs/0
multi/final/20220916_cci4au7nu1ibc2ta5j7g.dgut.dbs/0/dgut.db
multi/final/20220916_cci4au7nu1ibc2ta5j7g.dgut.dbs/0/dgut.db.children
multi/final/20220916_cci4au7nu1ibc2ta5j7g.dgut.dbs/1
multi/final/20220916_cci4au7nu1ibc2ta5j7g.dgut.dbs/1/dgut.db
multi/final/20220916_cci4au7nu1ibc2ta5j7g.dgut.dbs/1/dgut.db.children
multi/final/.dgut.dbs.updated

Before running tidy, the srcDir looks like:

multi/cci4fafnu1ia052l75sg
multi/cci4fafnu1ia052l75sg/go
multi/cci4fafnu1ia052l75sg/go/cci4fafnu1ia052l75t0
multi/cci4fafnu1ia052l75sg/go/cci4fafnu1ia052l75t0/walk.1
multi/cci4fafnu1ia052l75sg/go/cci4fafnu1ia052l75t0/walk.2
[...]
multi/cci4fafnu1ia052l75sg/go/cci4fafnu1ia052l75t0/walk.1.log
multi/cci4fafnu1ia052l75sg/go/cci4fafnu1ia052l75t0/walk.1.stats
multi/cci4fafnu1ia052l75sg/go/cci4fafnu1ia052l75t0/walk.1.byusergroup
multi/cci4fafnu1ia052l75sg/go/cci4fafnu1ia052l75t0/walk.1.bygroup
multi/cci4fafnu1ia052l75sg/go/cci4fafnu1ia052l75t0/walk.1.dgut
[...]
multi/cci4fafnu1ia052l75sg/go/cci4fafnu1ia052l75t0/combine.log.gz
multi/cci4fafnu1ia052l75sg/go/cci4fafnu1ia052l75t0/combine.byusergroup.gz
multi/cci4fafnu1ia052l75sg/go/cci4fafnu1ia052l75t0/combine.bygroup
multi/cci4fafnu1ia052l75sg/go/cci4fafnu1ia052l75t0/combine.dgut.db
multi/cci4fafnu1ia052l75sg/go/cci4fafnu1ia052l75t0/combine.dgut.db/dgut.db
multi/cci4fafnu1ia052l75sg/go/cci4fafnu1ia052l75t0/combine.dgut.db/dgut.db.children
multi/cci4fafnu1ia052l75sg/go/cci4fafnu1ia052l75t0/combine.stats.gz
[]
multi/cci4fafnu1ia052l75sg/perl
multi/cci4fafnu1ia052l75sg/perl/cci4fafnu1ia052l75tg
multi/cci4fafnu1ia052l75sg/perl/cci4fafnu1ia052l75tg/walk.1
[...]
multi/cci4fafnu1ia052l75sg/base.dirs

 ******************************************************************************/

package tidy

import (
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

		multiUniqueDir := filepath.Join(srcDir, "cci4fafnu1ia052l75sg")
		interestBaseDir1 := filepath.Join(multiUniqueDir, "go")
		interestUniqueDir1 := filepath.Join(interestBaseDir1, "cci4fafnu1ia052l75t0")
		interestBaseDir2 := filepath.Join(multiUniqueDir, "perl")
		interestUniqueDir2 := filepath.Join(interestBaseDir2, "cci4fafnu1ia052l75tg")

		err = os.Mkdir(multiUniqueDir, modePermUser)
		So(err, ShouldBeNil)
		err = os.Mkdir(interestBaseDir1, modePermUser)
		So(err, ShouldBeNil)
		err = os.Mkdir(interestUniqueDir1, modePermUser)
		So(err, ShouldBeNil)
		err = os.Mkdir(interestBaseDir2, modePermUser)
		So(err, ShouldBeNil)
		err = os.Mkdir(interestUniqueDir2, modePermUser)
		So(err, ShouldBeNil)

		Convey("The combine.log.gz files are moved to uniquely named logs.gz files in the dest dir", func() {
			combineLog1 := filepath.Join(interestUniqueDir1, "combine.log.gz")
			emptyFile, errc := os.Create(combineLog1)
			So(errc, ShouldBeNil)
			err = emptyFile.Close()
			So(err, ShouldBeNil)
			combineLog2 := filepath.Join(interestUniqueDir2, "combine.log.gz")
			emptyFile, err = os.Create(combineLog2)
			So(err, ShouldBeNil)
			err = emptyFile.Close()
			So(err, ShouldBeNil)

			err = Up(srcDir, destDir, date)
			So(err, ShouldBeNil)

			_, err = os.Stat(combineLog1)
			So(err, ShouldNotBeNil)
			_, err = os.Stat(combineLog2)
			So(err, ShouldNotBeNil)

			finalLog1 := filepath.Join(destDir, date+"_go.cci4fafnu1ia052l75t0.cci4fafnu1ia052l75sg.logs.gz")
			_, err = os.Stat(finalLog1)
			So(err, ShouldBeNil)
			finalLog2 := filepath.Join(destDir, date+"_perl.cci4fafnu1ia052l75tg.cci4fafnu1ia052l75sg.logs.gz")
			_, err = os.Stat(finalLog2)
			So(err, ShouldBeNil)
		})

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
