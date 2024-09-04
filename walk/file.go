/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/wtsi-ssg/wrstat/v5/internal/encode"
)

const userOnlyPerm = 0700

// WriteError is an error received when trying to write strings to disk.
type WriteError struct {
	Err error
}

func (e *WriteError) Error() string { return e.Err.Error() }

func (e *WriteError) Unwrap() error { return e.Err }

// Files represents a collection of output files that can be written to in a
// round-robin.
type Files struct {
	files    []*os.File
	Paths    []string
	filesI   int
	filesMax int
	mu       sync.RWMutex
	mus      []sync.Mutex
}

// NewFiles returns a Files that has a WritePaths method that will return a
// PathsCallback function suitable for passing to New().
//
// This creates n output files in outDir, and writes the walk paths to those
// files 1 per line in a round-robin.
//
// The output file paths can be found in the Paths property.
//
// Be sure to Close() after you've finished walking.
func NewFiles(outDir string, n int) (*Files, error) {
	if err := os.MkdirAll(outDir, userOnlyPerm); err != nil {
		return nil, err
	}

	files := make([]*os.File, n)
	outPaths := make([]string, n)

	for i := range files {
		var err error

		path := filepath.Join(outDir, fmt.Sprintf("walk.%d", i+1))

		files[i], err = os.Create(path)
		if err != nil {
			return nil, err
		}

		outPaths[i] = path
	}

	return &Files{
		files:    files,
		Paths:    outPaths,
		filesMax: len(files),
		mus:      make([]sync.Mutex, len(files)),
	}, nil
}

// WritePaths returns a PathCallback function suitable for passing to New().
//
// Paths are written 1 per line to our output files in a round-robin.
//
// It will terminate the walk if writes to our output files fail.
func (f *Files) WritePaths() PathCallback {
	return func(entry *Dirent) error {
		return f.writePath(encode.Base64Encode(entry.Path))
	}
}

// writePath is a thread-safe way of writing the given path to our next output
// file. Returns a WriteError on failure to write to an output file.
func (f *Files) writePath(path string) error {
	f.mu.Lock()
	i := f.filesI
	f.filesI++

	if f.filesI == f.filesMax {
		f.filesI = 0
	}

	f.mu.Unlock()

	f.mus[i].Lock()
	defer f.mus[i].Unlock()

	_, err := io.WriteString(f.files[i], path+"\n")
	if err != nil {
		err = &WriteError{Err: err}
	}

	return err
}

// Close should be called after Walk()ing to close all the output files.
func (f *Files) Close() error {
	for _, file := range f.files {
		if err := file.Close(); err != nil {
			return err
		}
	}

	return nil
}
