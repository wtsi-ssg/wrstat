/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * Partially based on github.com/MichaelTJones/walk
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
// there are on it. It does 0 stat calls.

package walk

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/karrick/godirwalk"
)

const userOnlyPerm = 0700
const walkers = 16
const dirsChSize = 1024

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
	mu       sync.RWMutex
	mus      []sync.Mutex
	dirsCh   chan string
	active   sync.WaitGroup
	err      error
	errCB    ErrorCallback
	ended    bool
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
	dir = filepath.Clean(dir)

	w.errCB = cb
	w.dirsCh = make(chan string, dirsChSize)

	defer func() {
		w.mu.Lock()
		w.ended = true
		w.mu.Unlock()
		close(w.dirsCh)
	}()

	w.addDir(dir)

	for i := 0; i < walkers; i++ {
		go w.processDirs()
	}

	w.active.Wait()

	return w.err
}

// addDir adds the given dir to our channel for processDirs() to pick up.
func (w *Walker) addDir(dir string) {
	w.mu.RLock()
	if w.err != nil || w.ended {
		w.mu.RUnlock()

		return
	}

	w.active.Add(1)

	go func() {
		w.dirsCh <- dir
	}()

	w.mu.RUnlock()
}

// processDirs pulls from our dirsCh and calls processDir on each.
func (w *Walker) processDirs() {
	buffer := make([]byte, os.Getpagesize())

	for dir := range w.dirsCh {
		w.processDir(dir, buffer)
	}
}

// processDir gets the contents of the given directory, outputs paths to our
// output files, and adds directories to our dirsCh. The buffer is used to speed
// up reading directory contents.
func (w *Walker) processDir(dir string, buffer []byte) {
	defer func() {
		w.active.Done()
	}()

	if w.terminated() {
		return
	}

	subDirs, otherEntries, ok := w.getImmediateChildren(dir, buffer)
	if !ok {
		return
	}

	if err := w.writeEntries(append(otherEntries, dir)); err != nil {
		w.terminate(err)

		return
	}

	for _, subDir := range subDirs {
		w.addDir(subDir)
	}
}

// terminated returns true if one of our go routines has called terminate().
func (w *Walker) terminated() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.err != nil
}

// getImmediateChildren finds the immediate children of the given directory and
// returns any entries that are subdirectories, then any other entries. Any
// failure to read is passed to our errCB, but we don't return an error (just
// nil results and false).
func (w *Walker) getImmediateChildren(dir string, buffer []byte) ([]string, []string, bool) {
	children, err := godirwalk.ReadDirents(dir, buffer)
	if err != nil {
		w.errCB(dir, err)

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
func (w *Walker) writeEntries(paths []string) error {
	for _, path := range paths {
		if err := w.writePath(path); err != nil {
			w.errCB(path, err)

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

// terminate will store the err on self on the first call to terminate, and
// cause subsequent terminated() calls to return true.
func (w *Walker) terminate(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.err == nil {
		w.err = err
	}
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
