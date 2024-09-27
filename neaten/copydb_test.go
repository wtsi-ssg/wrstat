/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * Author: Ash Holland <ah37@sanger.ac.uk>
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
	"os"
	"path/filepath"
	"strconv"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestCopyDB(t *testing.T) {
	Convey("Given source and destination dgut db directories", t, func() {
		tdir := t.TempDir()
		sourceDir := filepath.Join(tdir, "source")
		destDir := filepath.Join(tdir, "dest")

		err := os.MkdirAll(sourceDir, modePermUser)
		So(err, ShouldBeNil)

		err = os.MkdirAll(destDir, modePermUser)
		So(err, ShouldBeNil)

		makeFakeDgutDB(t, sourceDir, 0)

		destSubFolders := 3
		for i := 0; i < destSubFolders; i++ {
			makeFakeDgutDB(t, destDir, i)
		}

		Convey("You can merge them as expected", func() {
			err = checkMergeWorks(t, sourceDir, destDir, destSubFolders+1)
			So(err, ShouldBeNil)
		})

		Convey("You can merge as expected with >10 directories in dest", func() {
			moreDestSubFolders := 12
			for i := destSubFolders; i < moreDestSubFolders; i++ {
				makeFakeDgutDB(t, destDir, i)
			}

			err = checkMergeWorks(t, sourceDir, destDir, moreDestSubFolders+1)
			So(err, ShouldBeNil)
		})

		Convey("You can merge as expected with >10 directories in source", func() {
			moreSourceSubFolders := 12
			for i := 1; i < moreSourceSubFolders; i++ {
				makeFakeDgutDB(t, sourceDir, i)
			}

			err = checkMergeWorks(t, sourceDir, destDir, destSubFolders+moreSourceSubFolders)
			So(err, ShouldBeNil)
		})
	})
}

func makeFakeDgutDB(t *testing.T, dir string, subFolderNum int) {
	t.Helper()

	subDir := subDir(dir, subFolderNum)

	err := os.MkdirAll(subDir, modePermUser)
	if err != nil {
		t.Fatalf("failed to make subdir: %s", err)
	}

	for _, basename := range []string{"dgut.db", "dgut.db.children"} {
		err = CreateFile(filepath.Join(subDir, basename))
		if err != nil {
			t.Fatalf("failed to make subdir: %s", err)
		}
	}
}

func subDir(dir string, subFolderNum int) string {
	return filepath.Join(dir, strconv.Itoa(subFolderNum))
}

func checkDgutDBDir(t *testing.T, dir string, subFolderNum int) error {
	t.Helper()

	subDir := subDir(dir, subFolderNum)

	dgutdbPath := filepath.Join(subDir, "dgut.db")

	_, err := os.Stat(dgutdbPath)
	if err != nil {
		return err
	}

	childPath := dgutdbPath + ".children"

	_, err = os.Stat(childPath)

	return err
}

func checkMergeWorks(t *testing.T, sourceDir, destDir string, expectedDirs int) error {
	t.Helper()

	err := MergeDGUTDBDirectories(sourceDir, destDir)
	if err != nil {
		return err
	}

	for i := 0; i < expectedDirs; i++ {
		err = checkDgutDBDir(t, destDir, i)
		if err != nil {
			return err
		}
	}

	return nil
}
