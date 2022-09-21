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
[...]
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

// Takes a set of bases and creates a directory out of them - effectively
// combines filepath.Join and os.Mkdir.
func createAndTestDir(root string, bases ...string) (string, error) {
	for _, base := range bases {
		if _, err := os.Stat(root); !os.IsNotExist(err) {
			root = filepath.Join(root, base)

			continue
		} else if err := os.Mkdir(root, modePermUser); err != nil {
			return root, err
		}

		root = filepath.Join(root, base)
	}

	err := os.Mkdir(root, modePermUser)

	return root, err
}

// Creates a file in the path provided by the user.
func createFile(fileName string) (os.File, error) {
	returnFile, err := os.Create(fileName)
	if err != nil {
		return *returnFile, err
	}

	err = returnFile.Close()

	return *returnFile, err
}

func TestTidy(t *testing.T) {
	date := "20220829"

	Convey("Given existing source and dest dirs you can tidy the source", t, func() {
		tmpDir := t.TempDir()
		srcDir := filepath.Join(tmpDir, "src")
		destDir := filepath.Join(tmpDir, "dest")

		srcUniversalPath := "cci4fafnu1ia052l75sg"
		srcUniquePathGo := "cci4fafnu1ia052l75t0"
		srcUniquePathPerl := "cci4fafnu1ia052l75tg"

		destUniversalPath := "cci4au7nu1ibc2ta5j7g"
		destUniquePathGo := "cci4au7nu1ibc2ta5j80"
		destUniquePathPerl := "cci4au7nu1ibc2ta5j8g"

		interestUniqueDir1, err := createAndTestDir(srcDir, srcUniversalPath, "go", srcUniquePathGo)
		So(err, ShouldBeNil)
		interestUniqueDir2, err := createAndTestDir(srcDir, srcUniversalPath, "perl", srcUniquePathPerl)
		So(err, ShouldBeNil)

		Convey("For each file suffix, it moves those files in the source dir to the dest dir", func() {
			fileSuffixes := [4]string{"stats.gz", "byusergroup.gz", "bygroup", "logs.gz"}

			for i := range fileSuffixes {
				// Create test file 1
				combineLog1 := filepath.Join(interestUniqueDir1, fileSuffixes[i])
				_, err = createFile(combineLog1)
				So(err, ShouldBeNil)

				// Create test file 2
				combineLog2 := filepath.Join(interestUniqueDir2, fileSuffixes[i])
				_, err = createFile(combineLog2)
				So(err, ShouldBeNil)

				err = Up(srcDir, destDir, date)
				So(err, ShouldBeNil)

				// The created files should no longer exist in the src dir
				_, err = os.Stat(combineLog1)
				So(err, ShouldNotBeNil)
				_, err = os.Stat(combineLog2)
				So(err, ShouldNotBeNil)

				// See if test file 1 has been appropriately moved and renamed
				finalLog1 := filepath.Join(destDir, date+"_go."+destUniquePathGo+"."+destUniversalPath+"."+fileSuffixes[i])
				_, err = createFile(finalLog1)
				So(err, ShouldBeNil)

				// See if test file 2 has been appropriately moved and renamed
				finalLog2 := filepath.Join(destDir, date+"_perl"+"."+destUniquePathPerl+"."+destUniversalPath+"."+fileSuffixes[i])
				_, err = createFile(finalLog2)
				So(err, ShouldBeNil)
			}
		})

		Convey("The permissions of a moved file should match those of a destination directory", func() {
			// Create a file
			srcFile := filepath.Join(interestUniqueDir1, "combine.dgut.db")
			_, err = createFile(srcFile)
			So(err, ShouldBeNil)

			// Get permissions of the file and the dest dir
			srcFilePerm, err := os.Stat(srcFile)
			So(err, ShouldBeNil)
			// destPerm, err := os.Stat(destDir)

			// Move file to dest dir
			err = Up(srcDir, destDir, date)
			So(err, ShouldBeNil)

			// Check permissions of the file are still the same as before
			endingFile := filepath.Join(destDir, date+"_"+destUniversalPath+".dgut.dbs")
			_, err = createFile(endingFile)
			So(err, ShouldBeNil)
			newFilePerm, err := os.Stat(endingFile)
			So(err, ShouldBeNil)
			So(srcFilePerm == newFilePerm, ShouldBeTrue) // Not quite sure how to assert that permissions should match each other
		})

		Convey("The combine.dgut.db directories are moved into the appropriate directory", func() {
			// Create file name
			startingFile := filepath.Join(interestUniqueDir1, "combine.dgut.db")

			// Create dir with this name
			_, err = createFile(startingFile)
			So(err, ShouldBeNil)

			// Call the function with file now in src dir
			err = Up(srcDir, destDir, date)
			So(err, ShouldBeNil)

			// Create corresponding file name in dest dir
			endingFile := filepath.Join(destDir, date+"_"+destUniversalPath+".dgut.dbs")

			// Check this file exists in dest dir
			_, err = os.Stat(endingFile)
			So(err, ShouldBeNil)
		})

		Convey("base.dirs file in the src dir is moved to a .basedirs file in dest dir", func() {
			// Create file name
			startingFile := filepath.Join(srcDir, srcUniversalPath, "base.dirs")

			// Create dir with this name
			_, err = createFile(startingFile)
			So(err, ShouldBeNil)

			// Call the function with file now in src dir
			err = Up(srcDir, destDir, date)
			So(err, ShouldBeNil)

			// Create corresponding file name in dest dir
			endingFile := filepath.Join(destDir, date+"_"+destUniversalPath+".basedirs")

			// Check this file exists in dest dir
			_, err = os.Stat(endingFile)
			So(err, ShouldBeNil)
		})

		Convey("Up deletes the source directory after the files have been moved", func() {
			err = Up(srcDir, destDir, date)
			So(err, ShouldBeNil)

			_, err = os.Stat(srcDir)
			So(err, ShouldNotBeNil)
		})

		Convey("It also works if the dest dir doesn't exist", func() {
			err := os.RemoveAll(destDir)
			So(err, ShouldBeNil)

			err = Up(srcDir, destDir, date)
			So(err, ShouldBeNil)

			_, err = os.Stat(destDir)
			So(err, ShouldBeNil)
		})

		Convey("It doesn't work if source dir doesn't exist", func() {
			err := os.RemoveAll(srcDir)
			So(err, ShouldBeNil)

			err = Up(srcDir, destDir, date)
			So(err, ShouldNotBeNil)

			_, err = os.Stat(srcDir)
			So(err, ShouldNotBeNil)
		})

		Convey("It doesn't work if source or dest is an incorrect relative path", func() {
			relDir := filepath.Join(tmpDir, "rel")
			err := os.Mkdir(relDir, modePermUser)
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

func createSourceDirectoryStructure(t *testing.T) {
	t.Helper()
}
