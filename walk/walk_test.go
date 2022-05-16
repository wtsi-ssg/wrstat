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
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const permNoWrite = 0500

func TestWalk(t *testing.T) {
	Convey("Given a directory to walk and an output directory", t, func() {
		walkDir, outDir, expectedPaths := prepareTestDirs(t)

		var mu sync.Mutex

		var walkErrors []error
		cb := func(_ string, err error) {
			mu.Lock()
			defer mu.Unlock()

			walkErrors = append(walkErrors, err)
		}

		Convey("You can output the paths to a file", func() {
			w, err := New(outDir, 1)
			So(err, ShouldBeNil)

			err = w.Walk(walkDir, cb)
			So(err, ShouldBeNil)

			outPath := filepath.Join(outDir, "walk.1")
			content, err := os.ReadFile(outPath)
			So(err, ShouldBeNil)

			found, dups, missing := checkPaths(string(content), expectedPaths)
			So(found, ShouldEqual, 81)
			So(dups, ShouldEqual, 0)
			So(missing, ShouldEqual, 0)
			So(len(walkErrors), ShouldEqual, 0)
		})

		Convey("You can output the paths to multiple files", func() {
			n := 4
			w, err := New(outDir, n)
			So(err, ShouldBeNil)

			err = w.Walk(walkDir, cb)
			So(err, ShouldBeNil)

			totalFound := 0

			outPaths := w.OutputPaths()

			for i := 1; i <= n+1; i++ {
				outPath := filepath.Join(outDir, fmt.Sprintf("walk.%d", i))
				content, errr := os.ReadFile(outPath)

				if i <= n {
					So(errr, ShouldBeNil)

					So(outPaths[i-1], ShouldEqual, outPath)

					found, dups, _ := checkPaths(string(content), expectedPaths)

					So(found, ShouldBeGreaterThanOrEqualTo, 20)
					So(dups, ShouldEqual, 0)
					totalFound += found
				} else {
					So(errr, ShouldNotBeNil)
				}
			}

			So(totalFound, ShouldEqual, 81)
			So(len(walkErrors), ShouldEqual, 0)

			err = w.Close()
			So(err, ShouldBeNil)

			err = w.files[0].Close()
			So(err, ShouldNotBeNil)

			err = w.Close()
			So(err, ShouldNotBeNil)
		})

		Convey("Write errors during a walk are reported and the walk terminated", func() {
			w, err := New(outDir, 1)
			So(err, ShouldBeNil)

			err = w.files[0].Close()
			So(err, ShouldBeNil)

			err = w.Walk(walkDir, cb)
			So(err, ShouldNotBeNil)
			So(len(walkErrors), ShouldEqual, 1)

			var writeError *WriteError
			So(errors.As(walkErrors[0], &writeError), ShouldBeTrue)

			werr := walkErrors[0].(*WriteError) //nolint:errcheck,errorlint,forcetypeassert
			So(werr.Unwrap(), ShouldEqual, werr.Err)

			err = w.walkSubDirs([]string{walkDir}, cb)
			So(err, ShouldNotBeNil)
			So(len(walkErrors), ShouldEqual, 2)
		})

		Convey("Read errors during a walk are reported and the path skipped", func() {
			w, err := New(outDir, 1)
			So(err, ShouldBeNil)

			err = w.Walk("/root", cb)
			So(err, ShouldBeNil)
			So(len(walkErrors), ShouldEqual, 1)
			var writeError *WriteError
			So(errors.As(walkErrors[0], &writeError), ShouldBeFalse)

			outPath := filepath.Join(outDir, "walk.1")
			_, err = os.ReadFile(outPath)
			So(err, ShouldBeNil)

			err = w.walkSubDirs([]string{"/root"}, cb)
			So(err, ShouldBeNil)
			So(len(walkErrors), ShouldEqual, 2)
		})
	})

	Convey("You can't make a Walker on a bad directory", t, func() {
		_, err := New("/foo", 1)
		So(err, ShouldNotBeNil)

		tmpDir := t.TempDir()
		outDir := filepath.Join(tmpDir, "out")
		err = os.Mkdir(outDir, permNoWrite)
		So(err, ShouldBeNil)

		_, err = New(outDir, 1)
		So(err, ShouldNotBeNil)
	})
}

// prepareTestDirs creates a temporary directory filled with files to walk, and
// an empty directory you can output to. Also returns all the paths created in a
// map.
func prepareTestDirs(t *testing.T) (string, string, map[string]int) {
	t.Helper()
	tmpDir := t.TempDir()

	walkDir := filepath.Join(tmpDir, "walk")
	outDir := filepath.Join(tmpDir, "out")

	if err := os.Mkdir(walkDir, os.ModePerm); err != nil {
		t.Fatalf("mkdir failed: %s", err)
	}

	if err := os.Mkdir(outDir, os.ModePerm); err != nil {
		t.Fatalf("mkdir failed: %s", err)
	}

	paths := make(map[string]int)
	paths[walkDir] = 0
	fillDirWithFiles(t, walkDir, 4, paths)

	return walkDir, outDir, paths
}

// fillDirWithFiles fills the given directory with files, size dirs wide and
// deep. Adds all paths created to the given map, setting values to 0.
func fillDirWithFiles(t *testing.T, dir string, size int, paths map[string]int) {
	t.Helper()

	for i := 1; i <= size; i++ {
		base := fmt.Sprintf("%d", i)
		path := filepath.Join(dir, base)
		filePath := path + ".file"

		paths[path] = 0
		paths[filePath] = 0

		if err := os.WriteFile(filePath, []byte(base), userOnlyPerm); err != nil {
			t.Fatalf("file creation failed: %s", err)
		}

		if err := os.Mkdir(path, os.ModePerm); err != nil {
			t.Fatalf("mkdir failed: %s", err)
		}

		if size-1 > 1 {
			fillDirWithFiles(t, path, size-1, paths)
		}
	}
}

// checkPaths parses the string content of a Walk() output file and marks how
// many times given paths were found in the map, returning numbers found,
// duplicated and not found.
func checkPaths(content string, paths map[string]int) (found, dups, missing int) {
	scanner := bufio.NewScanner(strings.NewReader(content))

	for scanner.Scan() {
		path := scanner.Text()
		if n, exists := paths[path]; exists {
			n++
			if n > 1 {
				dups++
			}

			paths[path] = n

			found++
		}
	}

	for _, n := range paths {
		if n == 0 {
			missing++
		}
	}

	return found, dups, missing
}
