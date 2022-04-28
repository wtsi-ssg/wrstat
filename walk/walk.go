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
	outDir string
	files  []*os.File
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
	i := 0
	max := len(w.files)

	var writeError *WriteError

	return godirwalk.Walk(dir, &godirwalk.Options{
		Callback: func(path string, de *godirwalk.Dirent) error {
			_, err := w.files[i].WriteString(path + "\n")
			i++
			if i == max {
				i = 0
			}

			if err != nil {
				err = &WriteError{Err: err}
			}

			return err
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
