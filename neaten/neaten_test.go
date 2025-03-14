/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * Author: Kyle Mace <km34@sanger.ac.uk>
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

package neaten

import (
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

const modePermUser = 0770

func TestTidy(t *testing.T) {
	srcUniversal := "cci4fafnu1ia052l75sg"

	Convey("Given existing source and dest dirs you can tidy the source", t, func() {
		tmpDir := t.TempDir()
		srcDir := filepath.Join(tmpDir, "src", srcUniversal)
		dest := filepath.Join(tmpDir, "dest")
		final := filepath.Join(dest, "final")

		combineSuffixes := buildSrcDir(srcDir)

		createTestDirWithDifferentPerms(dest)

		test := Tidy{
			SrcDir:  srcDir,
			DestDir: final,

			CombineFileSuffixes: combineSuffixes,

			CombineFileGlobPattern:  "%s/%s",
			WalkFilePathGlobPattern: "%s/*%s",

			DestDirPerms: modePermUser,
		}

		disableDeletion := false

		err := test.Up(disableDeletion)
		So(err, ShouldBeNil)

		Convey("And the combine files are moved from the source dir to the dest dir", func() {
			combineFileSuffixes := []string{"logs.gz", "stats.gz"}

			for _, suffix := range combineFileSuffixes {
				final1 := filepath.Join(final, suffix)
				_, err = os.Stat(final1)
				So(err, ShouldBeNil)
			}
		})

		Convey("And the mtime of the final directory matches the oldest mtime of the walk log files", func() {
			err = os.RemoveAll(tmpDir)
			So(err, ShouldBeNil)

			buildSrcDir(srcDir)

			createTestDirWithDifferentPerms(final)

			newMtimeFile := filepath.Join(srcDir, "walk.1.log")
			expectedMTime := time.Date(2006, time.April, 1, 3, 4, 5, 0, time.UTC)
			expectedATime := time.Date(2007, time.March, 2, 4, 5, 6, 0, time.UTC)

			err = os.Chtimes(newMtimeFile, expectedATime, expectedMTime)
			So(err, ShouldBeNil)

			err = test.Up(disableDeletion)
			So(err, ShouldBeNil)

			finalMTime := getMTime(final)

			So(finalMTime, ShouldEqual, expectedMTime)
		})

		Convey("And the moved file permissions match those of the dest dir", func() {
			destDirPerm, errs := os.Stat(dest)
			So(errs, ShouldBeNil)

			err = filepath.WalkDir(final, func(path string, d fs.DirEntry, err error) error {
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

			buildSrcDir(srcDir)

			createTestDirWithDifferentPerms(final)

			err = test.Up(disableDeletion)
			So(err, ShouldBeNil)

			_, err = os.Stat(srcDir)
			So(err, ShouldNotBeNil)
		})

		Convey("And up does not delete the source directory after the files have been moved if the arg is false", func() {
			err = os.RemoveAll(tmpDir)
			So(err, ShouldBeNil)

			buildSrcDir(srcDir)

			createTestDirWithDifferentPerms(final)

			err = test.Up(true)
			So(err, ShouldBeNil)

			_, err = os.Stat(srcDir)
			So(err, ShouldBeNil)
		})

		Convey("And it also works if the dest dir doesn't exist", func() {
			err := os.RemoveAll(final)
			So(err, ShouldBeNil)

			err = os.RemoveAll(srcDir)
			So(err, ShouldBeNil)

			buildSrcDir(srcDir)

			err = test.Up(disableDeletion)
			So(err, ShouldBeNil)

			_, err = os.Stat(final)
			So(err, ShouldBeNil)
		})

		Convey("And it doesn't work if source dir doesn't exist", func() {
			err := os.RemoveAll(srcDir)
			So(err, ShouldBeNil)

			err = test.Up(disableDeletion)
			So(err, ShouldNotBeNil)

			_, err = os.Stat(srcDir)
			So(err, ShouldNotBeNil)
		})

		Convey("And it doesn't work if source or dest is an incorrect relative path", func() {
			err := os.RemoveAll(final)
			So(err, ShouldBeNil)

			err = os.RemoveAll(srcDir)
			So(err, ShouldBeNil)

			buildSrcDir(srcDir)

			relDir := filepath.Join(tmpDir, "rel")
			err = os.MkdirAll(relDir, modePermUser)
			So(err, ShouldBeNil)

			err = os.Chdir(relDir)
			So(err, ShouldBeNil)

			err = os.RemoveAll(relDir)
			So(err, ShouldBeNil)

			test.SrcDir = "../src/" + srcUniversal
			test.DestDir = "../dest/final"

			err = test.Up(disableDeletion)
			So(err, ShouldNotBeNil)
		})
	})
}

func buildSrcDir(srcDir string) map[string]string {
	walkFileSuffixes := []string{"log", "stats"}
	combineFileSuffixes := []string{"combine.log.gz", "combine.stats.gz"}

	for i := range walkFileSuffixes {
		createTestPath([]string{srcDir}, "walk.1."+walkFileSuffixes[i])
	}

	for i := range combineFileSuffixes {
		createTestPath([]string{srcDir}, combineFileSuffixes[i])
	}

	inputOutputCombineSuffixes := map[string]string{
		combineFileSuffixes[0]: "logs.gz",
		combineFileSuffixes[1]: "stats.gz",
	}

	return inputOutputCombineSuffixes
}

// createTestPath takes a set of subdirectory names and an optional file
// basename and creates a directory and empty file out of them. Returns the
// directory.
func createTestPath(dirs []string, basename ...string) string {
	wholeDir := filepath.Join(dirs...)

	err := os.MkdirAll(wholeDir, modePermUser)
	So(err, ShouldBeNil)

	if len(basename) == 1 {
		err = CreateFile(filepath.Join(wholeDir, basename[0]))
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

// permissionsAndOwnershipSame takes two fileinfos and returns whether their
// permissions and ownerships are the same.
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
	aRW := a.Mode() & modeRW
	bRW := b.Mode() & modeRW

	return aRW == bRW
}

func TestTouch(t *testing.T) {
	Convey("Touch updates a file's a and mtime to now, in local time", t, func() {
		tdir := t.TempDir()
		path := filepath.Join(tdir, "file")

		err := CreateFile(path)
		So(err, ShouldBeNil)

		before := time.Now().Add(-10 * time.Second)
		err = os.Chtimes(path, before, before)
		So(err, ShouldBeNil)

		recent := time.Now()
		err = Touch(path)
		So(err, ShouldBeNil)

		info, err := os.Stat(path)
		So(err, ShouldBeNil)

		a := info.Sys().(*syscall.Stat_t).Atim //nolint:forcetypeassert
		atime := time.Unix(a.Sec, a.Nsec)

		So(atime, ShouldHappenAfter, recent)
		So(info.ModTime(), ShouldHappenAfter, recent)
		So(atime, ShouldEqual, info.ModTime())
	})
}
func TestDeleteAllPrefixedFiles(t *testing.T) {
	Convey("Only files with a matching prefix should be deleted", t, func() {
		tdir := t.TempDir()
		for _, name := range []string{"aaa", "aab", "baa"} {
			path := filepath.Join(tdir, name)
			err := CreateFile(path)
			So(err, ShouldBeNil)
		}

		err := DeleteAllPrefixedDirEntries(tdir, "a")
		So(err, ShouldBeNil)

		entries, err := os.ReadDir(tdir)
		So(err, ShouldBeNil)
		So(len(entries), ShouldEqual, 1)
		So(entries[0].Name(), ShouldEqual, "baa")
	})
}
