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

// package walk is used to quickly walk a filesystem to just see what paths
// there are on it.

package walk

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/karrick/godirwalk"
)

const userOnlyPerm = 0700

// WriteError is an error received when trying to write discovered paths to
// disk.
type WriteError struct {
	Err error
}

func (e *WriteError) Error() string { return e.Err.Error() }

func (e *WriteError) Unwrap() error { return e.Err }

// Walker can be used to quickly walk a filesystem to just see what paths there
// are on it.
type Walker struct {
	outDir   string
	files    []*os.File
	filesI   int
	filesMax int
	mu       sync.Mutex
	mus      []sync.Mutex
}

// New creates a new Walker that can Walk() a filesystem and write all the
// encountered paths to the given number of output files in the given output
// directory. The output files are created and opened ready for a Walk(). Any
// error during that process is also returned.
func New(outDir string, numOutputFiles int) (*Walker, error) {
	w := &Walker{
		outDir: outDir,
	}

	err := w.createOutputFiles(numOutputFiles)

	return w, err
}

// createOutputFiles creates the given number of output files ready for writing
// to.
func (w *Walker) createOutputFiles(n int) error {
	if err := os.MkdirAll(w.outDir, userOnlyPerm); err != nil {
		return err
	}

	files := make([]*os.File, n)

	for i := range files {
		var err error

		files[i], err = w.createOutputFile(i + 1)
		if err != nil {
			return err
		}
	}

	w.files = files
	w.filesMax = len(files)
	w.mus = make([]sync.Mutex, len(files))

	return nil
}

// createOutputFile creates an output file ready for writing to.
func (w *Walker) createOutputFile(i int) (*os.File, error) {
	return os.Create(filepath.Join(w.outDir, fmt.Sprintf("walk.%d", i)))
}

// ErrorCallback is a callback function you supply Walker.Walk(), and it
// will be provided problematic paths encountered during the walk.
type ErrorCallback func(path string, err error)

// Walk will discover all the paths nested under the given dir, and output them
// 1 per line to walk.* files in our outDir. Be sure to Close() after you've
// finished walking.
//
// The given callback will be called every time there's an error handling a file
// during the walk. Errors writing to an output file will result in the walk
// terminating early and this method returning the error; other kinds of errors
// will mean the path isn't output, but the walk will continue and this method
// won't return an error.
func (w *Walker) Walk(dir string, cb ErrorCallback) error {
	subDirs, otherEntries, ok := w.getImmediateChildren(dir, cb)
	if !ok {
		return nil
	}

	err := w.writeEntries(append(otherEntries, dir), cb)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	errCh := make(chan error, len(subDirs))

	for _, dir := range subDirs {
		wg.Add(1)

		go func(thisDir string) {
			defer wg.Done()

			werr := w.walkDir(thisDir, cb)
			errCh <- werr
		}(dir)
	}

	wg.Wait()

	for range subDirs {
		gerr := <-errCh
		if gerr != nil {
			return gerr
		}
	}

	return nil
}

// getImmediateChildren finds the immediate children of the given directory
// and returns any entries that are subdirectories, then any other entries. Like
// walkDir(), any failure to read is passed to the given callback, but we don't
// return an error (just nil results and false).
func (w *Walker) getImmediateChildren(dir string, cb ErrorCallback) ([]string, []string, bool) {
	children, err := godirwalk.ReadDirents(dir, nil)
	if err != nil {
		cb(dir, err)

		return nil, nil, false
	}

	var subDirs, otherEntries []string

	for _, child := range children {
		path := filepath.Join(dir, child.Name())

		if child.ModeType().IsDir() {
			subDirs = append(subDirs, path)
		} else {
			otherEntries = append(otherEntries, path)
		}
	}

	return subDirs, otherEntries, true
}

// writeEntries writes the given paths to our output files.
func (w *Walker) writeEntries(paths []string, cb ErrorCallback) error {
	for _, path := range paths {
		if err := w.writePath(path); err != nil {
			cb(path, err)

			return err
		}
	}

	return nil
}

// writePath is a thread-safe way of writing the given path to our next output
// file. Returns a WriteError on failure to write to an output file.
func (w *Walker) writePath(path string) error {
	w.mu.Lock()
	i := w.filesI
	w.filesI++

	if w.filesI == w.filesMax {
		w.filesI = 0
	}

	w.mu.Unlock()

	w.mus[i].Lock()
	defer w.mus[i].Unlock()

	_, err := w.files[i].WriteString(path + "\n")
	if err != nil {
		err = &WriteError{Err: err}
	}

	return err
}

// walkDir walks the given directory, writing the paths to entries found to our
// output files. Ends the walk if we fail to write to an output file, skips
// entries we can't read. All errors are supplied to the given error callback.
func (w *Walker) walkDir(dir string, cb ErrorCallback) error {
	var writeError *WriteError

	return godirwalk.Walk(dir, &godirwalk.Options{
		Callback: func(path string, de *godirwalk.Dirent) error {
			return w.writePath(path)
		},
		ErrorCallback: func(path string, err error) godirwalk.ErrorAction {
			cb(path, err)

			if errors.As(err, &writeError) {
				return godirwalk.Halt
			}

			return godirwalk.SkipNode
		},
		Unsorted: true,
	})
}

// Close should be called after Walk()ing to close all the output files.
func (w *Walker) Close() error {
	for _, file := range w.files {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}

// OutputPaths gives you the paths to the Walk() output files.
func (w *Walker) OutputPaths() []string {
	outPaths := make([]string, len(w.files))

	for i, file := range w.files {
		outPaths[i] = file.Name()
	}

	return outPaths
}
