/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
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
	"os"
	"path/filepath"
	"sync"

	"github.com/wtsi-hgi/godirwalk"
)

const walkers = 16
const dirsChSize = 1024

// PathCallback is a callback used by Walker.Walk() that receives a directory
// entry containing the path, inode and file type each time it's called. It
// should only return an error if you can no longer cope with receiving more
// paths, and wish to terminate the Walk.
type PathCallback func(entry *Dirent) error

// Walker can be used to quickly walk a filesystem to just see what paths there
// are on it.
type Walker struct {
	cb             PathCallback
	sendDirs       bool
	ignoreSymlinks bool
	dirsCh         chan string
	active         sync.WaitGroup
	err            error
	errCB          ErrorCallback
	mu             sync.RWMutex
	ended          bool
}

// New creates a new Walker that can Walk() a filesystem and send all the
// encountered paths to the given PathCallback.
//
// Set includeDirs to true to have your PathCallback receive directory paths in
// addition to file paths.
//
// Set ignoreSymlinks to true to have symlinks not be sent do your PathCallback.
func New(cb PathCallback, includDirs, ignoreSymlinks bool) *Walker {
	return &Walker{
		cb:             cb,
		sendDirs:       includDirs,
		ignoreSymlinks: ignoreSymlinks,
	}
}

// ErrorCallback is a callback function you supply Walker.Walk(), and it
// will be provided problematic paths encountered during the walk.
type ErrorCallback func(path string, err error)

// Walk will discover all the paths nested under the given dir, and send them to
// our PathCallback.
//
// The given error callback will be called every time there's an error handling
// a file during the walk. Errors writing to an output file will result in the
// walk terminating early and this method returning the error; other kinds of
// errors will mean the path isn't output, but the walk will continue and this
// method won't return an error.
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
	defer w.mu.RUnlock()

	if w.err != nil || w.ended {
		return
	}

	w.active.Add(1)

	go func() {
		w.dirsCh <- dir
	}()
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

	children, ok := w.getImmediateChildren(dir, buffer)
	if !ok {
		return
	}

	if w.sendDirs {
		children = append(children, newDirentForDirectoryPath(dir))
	}

	for _, entry := range children {
		if ok := w.handleDirent(entry, dir); !ok {
			return
		}
	}
}

// terminated returns true if one of our go routines has called terminate().
func (w *Walker) terminated() bool {
	w.mu.RLock()
	defer w.mu.RUnlock()

	return w.err != nil
}

// getImmediateChildren finds the immediate children of the given directory and
// returns the entries. Any failure to read is passed to our errCB, but we don't
// return an error (just nil results and false).
func (w *Walker) getImmediateChildren(dir string, buffer []byte) ([]*Dirent, bool) {
	children, err := godirwalk.ReadDirents(dir, buffer)
	if err != nil {
		w.errCB(dir, err)

		return nil, false
	}

	entries := make([]*Dirent, 0, len(children))

	for _, child := range children {
		if w.ignoreSymlinks && child.IsSymlink() {
			continue
		}

		entries = append(entries, &Dirent{
			Path:  filepath.Join(dir, child.Name()),
			Type:  child.ModeType(),
			Inode: child.Inode(),
		})
	}

	return entries, true
}

// handleDirent calls addDir() for directories (except for the given parent dir
// which was already done), and the path callback for files.
//
// Returns false if the path callback failed.
func (w *Walker) handleDirent(entry *Dirent, parentDir string) bool {
	if entry.IsDir() && entry.Path != parentDir {
		w.addDir(entry.Path)

		return true
	}

	if err := w.cb(entry); err != nil {
		w.errCB(entry.Path, err)
		w.terminate(err)

		return false
	}

	return true
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
