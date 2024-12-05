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
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
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

type pathRequest struct {
	path     *FilePath
	response chan []Dirent
}

var pathRequestPool = sync.Pool{ //nolint:gochecknoglobals
	New: func() any {
		return &pathRequest{
			response: make(chan []Dirent),
		}
	},
}

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
	requestCh := make(chan *pathRequest)
	sortedRequestCh := make(chan *pathRequest)
	direntCh := make(chan Dirent, dirsChSize)
	flowControl := newController()
	ctx, stop := context.WithCancel(context.Background())

	for range walkers {
		go w.handleDirReads(ctx, sortedRequestCh, errCB)
	}

	go func() {
		walkDirectory(ctx, newDirentForDirectoryPath(dir),
			flowControl, createPathRequestor(requestCh), w.sendDirs)
		close(direntCh)
	}()

	go sortPathRequests(ctx, requestCh, sortedRequestCh)
	go flowControl.PassControl(direntCh)

	defer stop()

	return w.sendDirentsToPathCallback(direntCh)
}

func createPathRequestor(requestCh chan *pathRequest) func(*FilePath) []Dirent {
	return func(path *FilePath) []Dirent {
		pr := pathRequestPool.Get().(*pathRequest) //nolint:errcheck,forcetypeassert
		defer pathRequestPool.Put(pr)

		pr.path = path

		requestCh <- pr

		return <-pr.response
	}
}

func (w *Walker) sendDirentsToPathCallback(direntCh <-chan Dirent) error {
	for dirent := range direntCh {
		if err := w.pathCB(&dirent); err != nil {
			return err
		}
	}

	return nil
}

type heap []*pathRequest

func pathCompare(a, b *pathRequest) int {
	return strings.Compare(b.path.string(), a.path.string())
}

func (h *heap) Insert(req *pathRequest) {
	pos, _ := slices.BinarySearchFunc(*h, req, pathCompare)
	*h = slices.Insert(*h, pos, req)
}

func (h heap) Top() *pathRequest {
	return h[len(h)-1]
}

func (h *heap) Pop() {
	*h = (*h)[:len(*h)-1]
}

func (h *heap) Push(req *pathRequest) {
	*h = append(*h, req)
}

func sortPathRequests(ctx context.Context, requestCh <-chan *pathRequest, //nolint:gocyclo
	sortedRequestCh chan<- *pathRequest) {
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

func (w *Walker) handleDirReads(ctx context.Context, requests chan *pathRequest, errCB ErrorCallback) {
	buffer := make([]byte, os.Getpagesize())

Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case request := <-requests:
			children, err := godirwalk.ReadDirents(request.path.string(), buffer)
			if err != nil {
				errCB(string(request.path.Bytes()), err)
			}

			request.response <- w.childrenToDirents(children, request.path)
		}
	}
}

func (w *Walker) childrenToDirents(children godirwalk.Dirents, parent *FilePath) []Dirent {
	dirents := make([]Dirent, 0, len(children))

	for _, child := range children {
		dirent := Dirent{
			Path:  parent.sub(child),
			Type:  child.ModeType(),
			Inode: child.Inode(),
		}

		if w.ignoreSymlinks && dirent.IsSymlink() {
			continue
		}

		dirents = append(dirents, dirent)
	}

	sort.Slice(dirents, func(i, j int) bool {
		return dirents[i].Path.string() < dirents[j].Path.string()
	})

	return dirents
}

type flowController struct {
	controller chan chan<- Dirent
}

func newController() *flowController {
	return controllerPool.Get().(*flowController) //nolint:forcetypeassert
}

func (f *flowController) GetControl() chan<- Dirent {
	return <-f.controller
}

func (f *flowController) PassControl(control chan<- Dirent) {
	f.controller <- control
	<-f.controller
}

func (f *flowController) EndControl() {
	f.controller <- nil
	controllerPool.Put(f)
}

var controllerPool = sync.Pool{ //nolint:gochecknoglobals
	New: func() any {
		return &flowController{
			controller: make(chan chan<- Dirent),
		}
	},
}

func walkDirectory(ctx context.Context, dirent Dirent,
	flowControl *flowController, request func(*FilePath) []Dirent, sendDirs bool) {
	children := request(dirent.Path)
	childChans := make([]*flowController, len(children))

	for n, child := range children {
		childChans[n] = newController()

		if child.IsDir() {
			go walkDirectory(ctx, child, childChans[n], request, sendDirs)
		} else {
			go sendFileEntry(ctx, child, childChans[n])
		}
	}

	control := flowControl.GetControl()

	if sendDirs {
		sendEntry(ctx, dirent, control)
	}

	for _, childChan := range childChans {
		childChan.PassControl(control)
	}

	flowControl.EndControl()
}

func sendFileEntry(ctx context.Context, dirent Dirent, flowControl *flowController) {
	control := flowControl.GetControl()

	sendEntry(ctx, dirent, control)
	flowControl.EndControl()
}

func sendEntry(ctx context.Context, dirent Dirent, direntCh chan<- Dirent) {
	select {
	case <-ctx.Done():
		return
	case direntCh <- dirent:
	}
}
