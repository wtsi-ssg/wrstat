/*
******************************************************************************
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
*****************************************************************************
 */
package neaten

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

const modePermUser = 0700

func TestTidy(t *testing.T) { //nolint:gocognit
	date := "20220829"
	srcUniversal := "cci4fafnu1ia052l75sg"
	srcUniqueGo := "cci4fafnu1ia052l75t0"
	srcUniquePerl := "cci4fafnu1ia052l75tg"

	Convey("Given existing source and dest dirs you can tidy the source", t, func() {
		tmpDir := t.TempDir()
		srcDir := filepath.Join(tmpDir, "src", srcUniversal)
		destDir := filepath.Join(tmpDir, "dest", "final")
		interestUniqueDir1 := createTestPath([]string{srcDir, "go", srcUniqueGo})
		interestUniqueDir2 := createTestPath([]string{srcDir, "perl", srcUniquePerl})

		buildSrcDir(srcDir, srcUniqueGo, srcUniquePerl, interestUniqueDir1, interestUniqueDir2)

		createTestDirWithDifferentPerms(destDir)

		combineSuffixes := map[string]string{
			"combine.stats.gz":       "stats.gz",
			"combine.byusergroup.gz": "byusergroup.gz",
			"combine.bygroup":        "bygroup",
			"combine.log.gz":         "logs.gz"}

		dbSuffixes := map[string]string{
			"combine.dgut.db": "dgut.dbs"}

		baseSuffixes := map[string]string{
			"base.dirs": "basedirs"}

		test := Tidy{
			SrcDir:  srcDir,
			DestDir: destDir,
			Date:    date,

			CombineFileSuffixes: combineSuffixes,
			DBFileSuffixes:      dbSuffixes,
			BaseFileSuffixes:    baseSuffixes,

			CombineFileGlobPattern:  "%s/*/*/%s",
			DBFileGlobPattern:       "%s/*/*/%s",
			WalkFilePathGlobPattern: "%s/*/*/*%s",

			DestDirPerms: modePermUser,
		}

		err := test.Up()

		Convey("And the combine files are moved from the source dir to the dest dir", func() {
			combineFileSuffixes := [4]string{".logs.gz", ".byusergroup.gz", ".bygroup", ".stats.gz"}

			for i := range combineFileSuffixes {
				final1 := filepath.Join(destDir, date+"_go."+srcUniqueGo+"."+srcUniversal+combineFileSuffixes[i])
				_, err = os.Stat(final1)
				So(err, ShouldBeNil)

				final2 := filepath.Join(destDir, date+"_perl."+srcUniquePerl+"."+srcUniversal+combineFileSuffixes[i])
				_, err = os.Stat(final2)
				So(err, ShouldBeNil)
			}
		})

		Convey("And the the contents of the .basedirs and .dgut.dbs dir exist", func() {
			dbsPath := filepath.Join(destDir, date+"_"+srcUniversal)
			dbsSuffixes := [5]string{
				".basedirs",
				".dgut.dbs/0/dgut.db",
				".dgut.dbs/0/dgut.db.children",
				".dgut.dbs/1/dgut.db",
				".dgut.dbs/1/dgut.db.children"}

			for i := range dbsSuffixes {
				_, err = os.Stat(dbsPath + dbsSuffixes[i])
				So(err, ShouldBeNil)
			}
		})

		Convey("And the .dgut.dbs.updated file exists in the dest dir", func() {
			expectedFileName := filepath.Join(destDir, ".dgut.dbs.updated")

			_, err = os.Stat(expectedFileName)
			So(err, ShouldBeNil)
		})

		Convey("And the mtime of the .dgut.dbs file matches the oldest mtime of the walk log files", func() {
			err = os.RemoveAll(tmpDir)
			So(err, ShouldBeNil)

			buildSrcDir(srcDir, srcUniqueGo, srcUniquePerl, interestUniqueDir1, interestUniqueDir2)

			createTestDirWithDifferentPerms(destDir)

			newMtimeFile := filepath.Join(interestUniqueDir1, "walk.1.log")
			mTime := time.Date(2006, time.April, 1, 3, 4, 5, 0, time.UTC)
			aTime := time.Date(2007, time.March, 2, 4, 5, 6, 0, time.UTC)

			err = os.Chtimes(newMtimeFile, aTime, mTime)
			So(err, ShouldBeNil)

			err = test.Up()
			So(err, ShouldBeNil)

			dbsFileMTime := getMTime(filepath.Join(destDir, ".dgut.dbs.updated"))

			So(mTime, ShouldEqual, dbsFileMTime)
		})

		Convey("And the moved file permissions match those of the dest dir", func() {
			destDirPerm, errs := os.Stat(destDir)
			So(errs, ShouldBeNil)

			err = filepath.WalkDir(destDir, func(path string, d fs.DirEntry, err error) error {
				if err != nil {
					return err
				}

				pathPerm, err := os.Stat(path)
				if err != nil {
					return err
				}

				So(permissionsAndOwnershipSame(destDirPerm, pathPerm), ShouldBeTrue)

				return nil
			})
			So(err, ShouldBeNil)
		})

		Convey("And up deletes the source directory after the files have been moved", func() {
			err = os.RemoveAll(tmpDir)
			So(err, ShouldBeNil)

			buildSrcDir(srcDir, srcUniqueGo, srcUniquePerl, interestUniqueDir1, interestUniqueDir2)

			createTestDirWithDifferentPerms(destDir)

			err = test.Up()
			So(err, ShouldBeNil)

			_, err = os.Stat(srcDir)
			So(err, ShouldNotBeNil)
		})

		Convey("And it also works if the dest dir doesn't exist", func() {
			err := os.RemoveAll(destDir)
			So(err, ShouldBeNil)

			err = os.RemoveAll(srcDir)
			So(err, ShouldBeNil)

			buildSrcDir(srcDir, srcUniqueGo, srcUniquePerl, interestUniqueDir1, interestUniqueDir2)

			err = test.Up()
			So(err, ShouldBeNil)

			_, err = os.Stat(destDir)
			So(err, ShouldBeNil)
		})

		Convey("And it doesn't work if source dir doesn't exist", func() {
			err := os.RemoveAll(srcDir)
			So(err, ShouldBeNil)

			err = test.Up()
			So(err, ShouldNotBeNil)

			_, err = os.Stat(srcDir)
			So(err, ShouldNotBeNil)
		})

		Convey("And it doesn't work if source or dest is an incorrect relative path", func() {
			err := os.RemoveAll(destDir)
			So(err, ShouldBeNil)

			err = os.RemoveAll(srcDir)
			So(err, ShouldBeNil)

			buildSrcDir(srcDir, srcUniqueGo, srcUniquePerl, interestUniqueDir1, interestUniqueDir2)

			relDir := filepath.Join(tmpDir, "rel")
			err = os.MkdirAll(relDir, modePermUser)
			So(err, ShouldBeNil)

			err = os.Chdir(relDir)
			So(err, ShouldBeNil)

			err = os.RemoveAll(relDir)
			So(err, ShouldBeNil)

			test.SrcDir = "../src/" + srcUniversal
			test.DestDir = "../dest/final"

			err = test.Up()
			So(err, ShouldNotBeNil)
		})
	})
}

func buildSrcDir(srcDir, srcUniqueGo, srcUniquePerl, interestUniqueDir1, interestUniqueDir2 string) {
	walkFileSuffixes := [5]string{".log", ".stats", ".byusergroup", ".bygroup", ".dgut"}
	combineFileSuffixes := [4]string{"combine.log.gz", "combine.byusergroup.gz", "combine.bygroup", "combine.stats.gz"}

	for i := range walkFileSuffixes {
		createTestPath([]string{interestUniqueDir1}, "walk.1"+walkFileSuffixes[i])
		createTestPath([]string{interestUniqueDir2}, "walk.1"+walkFileSuffixes[i])
	}

	for i := range combineFileSuffixes {
		createTestPath([]string{interestUniqueDir1}, combineFileSuffixes[i])
		createTestPath([]string{interestUniqueDir2}, combineFileSuffixes[i])
	}

	goDBDir := []string{srcDir, "go", srcUniqueGo, "combine.dgut.db"}
	perlDBDir := []string{srcDir, "perl", srcUniquePerl, "combine.dgut.db"}
	combineDirSuffixes := [2]string{"dgut.db", "dgut.db.children"}

	for i := range combineDirSuffixes {
		createTestPath(goDBDir, combineDirSuffixes[i])
		createTestPath(perlDBDir, combineDirSuffixes[i])
	}

	createTestPath([]string{srcDir}, "base.dirs")
}

// createTestPath takes a set of subdirectory names and an optional file
// basename and creates a directory and empty file out of them. Returns the
// directory.
func createTestPath(dirs []string, basename ...string) string {
	wholeDir := filepath.Join(dirs...)

	err := os.MkdirAll(wholeDir, modePermUser)
	So(err, ShouldBeNil)

	if len(basename) == 1 {
		err = createFile(filepath.Join(wholeDir, basename[0]))
		So(err, ShouldBeNil)
	}

	return wholeDir
}

// createTestDirWithDifferentPerms creates the given directory with different
// group ownership and rw permissions than normal.
func createTestDirWithDifferentPerms(dir string) {
	err := os.MkdirAll(dir, 0777)
	So(err, ShouldBeNil)

	destUID := os.Getuid()
	destGroups, err := os.Getgroups()
	So(err, ShouldBeNil)

	err = os.Lchown(dir, destUID, destGroups[1])
	So(err, ShouldBeNil)
}

// getMTime takes a filePath and returns its Mtime.
func getMTime(filePath string) time.Time {
	FileInfo, err := os.Stat(filePath)
	So(err, ShouldBeNil)

	fileMTime := FileInfo.ModTime()

	return fileMTime
}

// permissionsAndOwnershipSame takes two fileinfos and returns whether their permissions and ownerships are the same.
func permissionsAndOwnershipSame(a, b fs.FileInfo) bool {
	return readWritePermissionsSame(a, b) && userAndGroupOwnershipSame(a, b)
}

// userAndGroupOwnershipSame tests if the given fileinfos have the same UID and
// GID.
func userAndGroupOwnershipSame(a, b fs.FileInfo) bool {
	aUID, aGID := getUIDAndGID(a)
	bUID, bGID := getUIDAndGID(b)

	return aUID == bUID && aGID == bGID
}

// matchReadWrite ensures that the given file with the current fileinfo has the
// same user,group,other read&write permissions as the desired fileinfo.
func readWritePermissionsSame(a, b fs.FileInfo) bool {
	aMode := a.Mode()
	aRW := aMode & modeRW
	bRW := b.Mode() & modeRW

	return aRW == bRW
}
