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
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"

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
	pathCB         PathCallback
	sendDirs       bool
	ignoreSymlinks bool
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
		pathCB:         cb,
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
func (w *Walker) Walk(dir string, errCB ErrorCallback) error {
	dir = filepath.Clean(dir) + "/"
	requestCh := make(chan pathRequest)
	sortedRequestCh := make(chan pathRequest)
	direntCh := make(chan Dirent)
	flowControlCh := make(chan chan<- Dirent)
	ctx, stop := context.WithCancel(context.Background())

	for range walkers {
		go w.handleDirReads(ctx, sortedRequestCh, errCB)
	}

	go func() {
		walkDirectory(ctx, Dirent{Path: dir, Type: fs.ModeDir}, flowControlCh, requestCh, w.sendDirs)
		close(direntCh)
	}()

	go sortPathRequests(ctx, requestCh, sortedRequestCh)

	flowControlCh <- direntCh

	defer stop()

	return w.sendDirentsToPathCallback(direntCh, errCB)
}

func (w *Walker) sendDirentsToPathCallback(direntCh <-chan Dirent, errCB ErrorCallback) error {
	for dirent := range direntCh {
		if err := w.pathCB(&dirent); err != nil {
			errCB(dirent.Path, err)

			return err
		}
	}

	return nil
}

type heap []pathRequest

func pathCompare(a, b pathRequest) int {
	return strings.Compare(b.path, a.path)
}

func (h *heap) Insert(req pathRequest) {
	pos, _ := slices.BinarySearchFunc(*h, req, pathCompare)
	*h = slices.Insert(*h, pos, req)
}

func (h heap) Top() pathRequest {
	return h[len(h)-1]
}

func (h *heap) Pop() {
	*h = (*h)[:len(*h)-1]
}

func (h *heap) Push(req pathRequest) {
	*h = append(*h, req)
}

func sortPathRequests(ctx context.Context, requestCh <-chan pathRequest, //nolint:gocyclo
	sortedRequestCh chan<- pathRequest) {
	var h heap

	for {
		if len(h) == 0 {
			select {
			case <-ctx.Done():
				return
			case req := <-requestCh:
				h.Push(req)
			}
		}

		select {
		case <-ctx.Done():
			return
		case req := <-requestCh:
			h.Insert(req)
		case sortedRequestCh <- h.Top():
			h.Pop()
		}
	}
}

func (w *Walker) handleDirReads(ctx context.Context, requests chan pathRequest, errCB ErrorCallback) {
	buffer := make([]byte, os.Getpagesize())

Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case request := <-requests:
			children, err := godirwalk.ReadDirents(request.path, buffer)
			if err != nil {
				errCB(request.path, err)
			}

			request.response <- w.childrenToDirents(children, request.path)
		}
	}
}

func (w *Walker) childrenToDirents(children godirwalk.Dirents, parent string) []Dirent {
	dirents := make([]Dirent, 0, len(children))

	for _, child := range children {
		dirent := Dirent{
			Path:  filepath.Join(parent, child.Name()),
			Type:  child.ModeType(),
			Inode: child.Inode(),
		}

		if dirent.IsDir() {
			dirent.Path += "/"
		}

		if w.ignoreSymlinks && dirent.IsSymlink() {
			continue
		}

		dirents = append(dirents, dirent)
	}

	sort.Slice(dirents, func(i, j int) bool {
		return dirents[i].Path < dirents[j].Path
	})

	return dirents
}

type pathRequest struct {
	path     string
	response chan<- []Dirent
}

func walkDirectory(ctx context.Context, dirent Dirent,
	flowControlCh chan chan<- Dirent, request chan<- pathRequest, sendDirs bool) {
	response := make(chan []Dirent)
	request <- pathRequest{dirent.Path, response}

	children := <-response
	childChans := make([]chan chan<- Dirent, len(children))

	for n, child := range children {
		childChans[n] = make(chan chan<- Dirent)

		if child.IsDir() {
			go walkDirectory(ctx, child, childChans[n], request, sendDirs)
		} else {
			go sendFileEntry(ctx, child, childChans[n])
		}
	}

	direntCh := <-flowControlCh

	if sendDirs {
		sendEntry(ctx, dirent, direntCh)
	}

	for _, childChan := range childChans {
		childChan <- direntCh
		<-childChan
	}

	close(flowControlCh)
}

func sendFileEntry(ctx context.Context, dirent Dirent, childChan chan chan<- Dirent) {
	direntCh := <-childChan

	sendEntry(ctx, dirent, direntCh)
	close(childChan)
}

func sendEntry(ctx context.Context, dirent Dirent, direntCh chan<- Dirent) {
	select {
	case <-ctx.Done():
		return
	case direntCh <- dirent:
	}
}
