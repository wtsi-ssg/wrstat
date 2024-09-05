/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
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
	"syscall"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/v5/internal/encode"
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
			found, dups, missing := testOutputToFiles(false, walkDir, outDir, cb, expectedPaths)
			So(found, ShouldEqual, 81)
			So(dups, ShouldEqual, 0)
			So(missing, ShouldEqual, 0)
			So(len(walkErrors), ShouldEqual, 0)
		})

		Convey("You can output the paths to multiple files", func() {
			n := 4
			files, err := NewFiles(outDir, n)
			So(err, ShouldBeNil)
			w := New(files.WritePaths(), true, false)

			err = w.Walk(walkDir, cb)
			So(err, ShouldBeNil)

			totalFound := 0

			for i := 1; i <= n+1; i++ {
				outPath := filepath.Join(outDir, fmt.Sprintf("walk.%d", i))
				content, errr := os.ReadFile(outPath)

				if i <= n {
					So(errr, ShouldBeNil)

					So(files.Paths[i-1], ShouldEqual, outPath)

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

			err = files.Close()
			So(err, ShouldBeNil)

			err = files.files[0].Close()
			So(err, ShouldNotBeNil)

			err = files.Close()
			So(err, ShouldNotBeNil)
		})

		Convey("You can ignore symlinks", func() {
			found, dups, missing := testOutputToFiles(true, walkDir, outDir, cb, expectedPaths)
			So(found, ShouldEqual, 80)
			So(dups, ShouldEqual, 0)
			So(missing, ShouldEqual, 1)
			So(len(walkErrors), ShouldEqual, 0)
		})

		Convey("Write errors during a walk are reported and the walk terminated", func() {
			files, err := NewFiles(outDir, 1)
			So(err, ShouldBeNil)
			w := New(files.WritePaths(), true, false)

			err = files.files[0].Close()
			So(err, ShouldBeNil)

			err = w.Walk(walkDir, cb)
			So(err, ShouldNotBeNil)
			lenErrors := len(walkErrors)
			So(lenErrors, ShouldBeGreaterThanOrEqualTo, 1)
			So(w.err, ShouldNotBeNil)

			var writeError *WriteError
			So(errors.As(walkErrors[0], &writeError), ShouldBeTrue)

			werr := walkErrors[0].(*WriteError) //nolint:errcheck,errorlint,forcetypeassert
			So(werr.Unwrap(), ShouldEqual, werr.Err)

			w.active.Add(1)
			w.processDir(walkDir, nil)
			So(len(walkErrors), ShouldEqual, lenErrors)

			w.addDir(walkDir)
		})

		Convey("Read errors during a walk are reported and the path skipped", func() {
			files, err := NewFiles(outDir, 1)
			So(err, ShouldBeNil)
			w := New(files.WritePaths(), true, false)

			err = w.Walk("/root", cb)
			So(err, ShouldBeNil)
			So(len(walkErrors), ShouldEqual, 1)
			var writeError *WriteError
			So(errors.As(walkErrors[0], &writeError), ShouldBeFalse)

			outPath := filepath.Join(outDir, "walk.1")
			_, err = os.ReadFile(outPath)
			So(err, ShouldBeNil)
		})

		Convey("You can get the inode of files in your callback", func() {
			tdir := t.TempDir()
			file := filepath.Join(tdir, "file")
			f, err := os.Create(file)
			So(err, ShouldBeNil)
			err = f.Close()
			So(err, ShouldBeNil)

			info, err := os.Stat(file)
			So(err, ShouldBeNil)
			u, ok := info.Sys().(*syscall.Stat_t)
			So(ok, ShouldBeTrue)
			So(u.Ino, ShouldNotEqual, 0)

			var gotInode uint64

			pcb := func(entry *Dirent) error {
				gotInode = entry.Inode

				return nil
			}

			w := New(pcb, false, false)
			err = w.Walk(tdir, cb)
			So(err, ShouldBeNil)
			So(len(walkErrors), ShouldEqual, 0)
			So(gotInode, ShouldEqual, u.Ino)
		})
	})

	Convey("You can't create output files in a bad directory", t, func() {
		_, err := NewFiles("/foo", 1)
		So(err, ShouldNotBeNil)

		tmpDir := t.TempDir()
		outDir := filepath.Join(tmpDir, "out")
		err = os.Mkdir(outDir, permNoWrite)
		So(err, ShouldBeNil)

		_, err = NewFiles(outDir, 1)
		So(err, ShouldNotBeNil)
	})
}

// prepareTestDirs creates a temporary directory filled with files to walk, and
// an empty directory you can output to. Also returns all the paths created in a
// map. One of the files will be a symlink to another of the files.
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

	replaceFileWithSymlink(t, paths)

	pathsEncoded := make(map[string]int, len(paths))
	for k, v := range paths {
		pathsEncoded[encode.Base64Encode(k)] = v
	}

	return walkDir, outDir, pathsEncoded
}

// fillDirWithFiles fills the given directory with files, size dirs wide and
// deep. Adds all paths created to the given map, setting values to 0.
func fillDirWithFiles(t *testing.T, dir string, size int, paths map[string]int) {
	t.Helper()

	for i := 1; i <= size; i++ {
		base := fmt.Sprintf("%d", i)
		path := filepath.Join(dir, base)

		filePath := path + ".file"
		if len(paths) == 1 {
			filePath += "\ntest"
		}

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

func replaceFileWithSymlink(t *testing.T, paths map[string]int) {
	t.Helper()

	dest := ""

	for path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat failed: %s", err)
		}

		if info.Mode().IsRegular() {
			if dest == "" {
				dest = path

				continue
			}

			removeAndSymlink(t, path, dest)

			break
		}
	}
}

func removeAndSymlink(t *testing.T, path, dest string) {
	t.Helper()

	err := os.Remove(path)
	if err != nil {
		t.Fatalf("remove failed: %s", err)
	}

	err = os.Symlink(dest, path)
	if err != nil {
		t.Fatalf("symlink failed: %s", err)
	}
}

func testOutputToFiles(ignoreSymlinks bool, walkDir, outDir string, cb ErrorCallback,
	expectedPaths map[string]int) (int, int, int) {
	files, err := NewFiles(outDir, 1)
	So(err, ShouldBeNil)

	w := New(files.WritePaths(), true, ignoreSymlinks)

	err = w.Walk(walkDir, cb)
	So(err, ShouldBeNil)

	outPath := filepath.Join(outDir, "walk.1")
	So(files.Paths[0], ShouldEqual, outPath)
	content, err := os.ReadFile(outPath)
	So(err, ShouldBeNil)

	return checkPaths(string(content), expectedPaths)
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
