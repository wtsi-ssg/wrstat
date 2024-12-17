/*******************************************************************************
 * Copyright (c) 2022, 2023, 2024 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 *         Michael Woolnough <mw31@sanger.ac.uk>
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
	"bytes"
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"syscall"
	"unsafe"
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
	Dirent
	next  *pathRequest
	ready sync.Mutex
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
	ctx, stop := context.WithCancel(context.Background())

	for range walkers {
		go w.handleDirReads(ctx, sortedRequestCh, requestCh, errCB, w.ignoreSymlinks)
	}

	go sortPathRequests(ctx, requestCh, sortedRequestCh)

	r := &pathRequest{
		Dirent: Dirent{
			Path:  NewFilePath(dir),
			Type:  fs.ModeDir,
			Inode: 0,
		},
	}

	r.ready.Lock()

	sortedRequestCh <- r

	defer stop()

	return w.sendDirentsToPathCallback(r)
}

func (w *Walker) sendDirentsToPathCallback(r *pathRequest) error {
	for ; r != nil; r = r.next {
		isDir := r.IsDir()

		if w.sendDirs || !isDir {
			if err := w.pathCB(&r.Dirent); err != nil {
				return err
			}
		}

		if isDir {
			r.ready.Lock()
		}
	}

	return nil
}

type heap []*pathRequest

func pathCompare(a, b *pathRequest) int {
	return b.Path.compare(&a.Path)
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

func (w *Walker) handleDirReads(ctx context.Context, sortedRequests, requestCh chan *pathRequest,
	errCB ErrorCallback, ignoreSymlinks bool) {
	buffer := make([]byte, os.Getpagesize())

	var pathBuffer [maxPathLength + 1]byte

Loop:
	for {
		select {
		case <-ctx.Done():
			break Loop
		case request := <-sortedRequests:
			l := len(request.Path.appendTo(pathBuffer[:0]))
			pathBuffer[l] = 0

			if err := scan(buffer, &pathBuffer[0], request, ignoreSymlinks); err != nil {
				errCB(string(pathBuffer[:l]), err)
			}

			go scanChildDirs(requestCh, request)
		}
	}
}

func scanChildDirs(requestCh chan *pathRequest, request *pathRequest) {
	for p, r := &request.Path, request.next; r != nil && r.Path.parent == p; {
		next := r.next

		if r.IsDir() {
			requestCh <- r
		}

		r = next
	}
}

type scanner struct {
	buffer, read []byte
	fh           int
	syscall.Dirent
	err error
}

func (s *scanner) Next() bool {
	for len(s.read) == 0 {
		n, err := syscall.ReadDirent(s.fh, s.buffer)
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}

			s.err = err

			return false
		}

		if n <= 0 {
			return false
		}

		s.read = s.buffer[:n]
	}

	copy((*[unsafe.Sizeof(syscall.Dirent{})]byte)(unsafe.Pointer(&s.Dirent))[:], s.read)
	s.read = s.read[s.Reclen:]

	return true
}

func (s *scanner) Get() (string, fs.FileMode, uint64) {
	mode := s.getMode()

	return s.getName(mode.IsDir()), mode, s.Ino
}

func (s *scanner) getName(isDir bool) string { //nolint:gocyclo
	n := s.Dirent.Name[:]
	name := *(*[]byte)(unsafe.Pointer(&n))

	l := bytes.IndexByte(name, 0)
	if l < 0 || l == 1 && s.Dirent.Name[0] == '.' || l == 2 && s.Dirent.Name[0] == '.' && s.Dirent.Name[1] == '.' {
		return ""
	}

	if isDir {
		s.Dirent.Name[l] = '/'
		l++
	}

	return string(name[:l])
}

func (s *scanner) getMode() fs.FileMode {
	switch s.Type {
	case syscall.DT_DIR:
		return fs.ModeDir
	case syscall.DT_LNK:
		return fs.ModeSymlink
	case syscall.DT_CHR:
		return fs.ModeDevice | fs.ModeCharDevice
	case syscall.DT_BLK:
		return fs.ModeDevice
	case syscall.DT_FIFO:
		return fs.ModeNamedPipe
	case syscall.DT_SOCK:
		return fs.ModeSocket
	}

	return 0
}

func scan(buffer []byte, path *byte, request *pathRequest, ignoreSymlinks bool) error {
	defer request.ready.Unlock()

	fh, err := open(path)
	if err != nil {
		return err
	}

	defer syscall.Close(fh)

	s := scanner{
		buffer: buffer,
		fh:     fh,
	}

	var last *pathRequest

	for s.Next() {
		name, mode, inode := s.Get()
		if inode == 0 || name == "" || ignoreSymlinks && mode&fs.ModeSymlink != 0 {
			continue
		}

		last = addDirent(request, last, name, mode, inode)
	}

	return nil
}

func open(path *byte) (int, error) {
	const atFDCWD = -0x64

	dfd := atFDCWD

	ifh, _, err := syscall.Syscall6(
		syscall.SYS_OPENAT,
		uintptr(dfd),
		uintptr(unsafe.Pointer(path)),
		uintptr(syscall.O_RDONLY),
		uintptr(0), 0, 0)
	if err != 0 {
		return 0, err
	}

	return int(ifh), nil
}

func addDirent(request, last *pathRequest, name string,
	mode fs.FileMode, inode uint64) *pathRequest {
	d := &pathRequest{
		Dirent: Dirent{
			Path: FilePath{
				parent: &request.Path,
				name:   name,
				depth:  request.Path.depth + 1,
			},
			Type:  mode,
			Inode: inode,
		},
	}

	if mode.IsDir() {
		d.ready.Lock()
	}

	return insertDirent(request, last, d)
}

func insertDirent(request, last, d *pathRequest) *pathRequest {
	if last == nil {
		return addFirst(request, d)
	} else if last.Path.name < d.Path.name {
		return insertAtEnd(last, d)
	}

	insertIntoList(request, last, d)

	return last
}

func addFirst(request, d *pathRequest) *pathRequest {
	d.next = request.next
	request.next = d

	return d
}

func insertAtEnd(last, d *pathRequest) *pathRequest {
	d.next = last.next
	last.next = d

	return d
}

func insertIntoList(request, last, d *pathRequest) {
	for curr := &request.next; curr != &last.next; curr = &(*curr).next {
		if d.Path.name < (*curr).Path.name {
			d.next = *curr
			*curr = d

			return
		}
	}
}
