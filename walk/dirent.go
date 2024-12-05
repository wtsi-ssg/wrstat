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
	"io/fs"
	"os"
	"sync"
	"unsafe"

	"github.com/wtsi-hgi/godirwalk"
)

var (
	filePathPool64   = sync.Pool{New: func() any { x := make(filePath, 0, 64); return &x }}   //nolint:gochecknoglobals,mnd,nlreturn,lll
	filePathPool128  = sync.Pool{New: func() any { x := make(filePath, 0, 128); return &x }}  //nolint:gochecknoglobals,mnd,nlreturn,lll
	filePathPool256  = sync.Pool{New: func() any { x := make(filePath, 0, 256); return &x }}  //nolint:gochecknoglobals,mnd,nlreturn,lll
	filePathPool512  = sync.Pool{New: func() any { x := make(filePath, 0, 512); return &x }}  //nolint:gochecknoglobals,mnd,nlreturn,lll
	filePathPool1024 = sync.Pool{New: func() any { x := make(filePath, 0, 1024); return &x }} //nolint:gochecknoglobals,mnd,nlreturn,lll
	filePathPool2048 = sync.Pool{New: func() any { x := make(filePath, 0, 2048); return &x }} //nolint:gochecknoglobals,mnd,nlreturn,lll
	filePathPool4096 = sync.Pool{New: func() any { x := make(filePath, 0, 4096); return &x }} //nolint:gochecknoglobals,mnd,nlreturn,lll
)

type filePath []byte

func newFilePathSize(size int) *filePath {
	switch {
	case size <= 64: //nolint:mnd
		return filePathPool64.Get().(*filePath) //nolint:forcetypeassert
	case size <= 128: //nolint:mnd
		return filePathPool128.Get().(*filePath) //nolint:forcetypeassert
	case size <= 256: //nolint:mnd
		return filePathPool256.Get().(*filePath) //nolint:forcetypeassert
	case size <= 512: //nolint:mnd
		return filePathPool512.Get().(*filePath) //nolint:forcetypeassert
	case size <= 1024: //nolint:mnd
		return filePathPool1024.Get().(*filePath) //nolint:forcetypeassert
	case size <= 2048: //nolint:mnd
		return filePathPool2048.Get().(*filePath) //nolint:forcetypeassert
	}

	return filePathPool4096.Get().(*filePath) //nolint:forcetypeassert
}

func newFilePath(path string) *filePath {
	c := newFilePathSize(len(path))
	c.writeString(path)

	return c
}

func (f *filePath) writeString(str string) {
	*f = append(*f, str...)
}

func (f *filePath) writeBytes(p []byte) {
	*f = append(*f, p...)
}

func (f *filePath) Done() { //nolint:gocyclo
	*f = (*f)[:0]

	switch cap(*f) {
	case 64: //nolint:mnd
		filePathPool64.Put(f)
	case 128: //nolint:mnd
		filePathPool128.Put(f)
	case 256: //nolint:mnd
		filePathPool256.Put(f)
	case 512: //nolint:mnd
		filePathPool512.Put(f)
	case 1024: //nolint:mnd
		filePathPool1024.Put(f)
	case 2048: //nolint:mnd
		filePathPool2048.Put(f)
	case 4096: //nolint:mnd
		filePathPool4096.Put(f)
	}
}

func (f *filePath) Sub(d *godirwalk.Dirent) *filePath {
	name := d.Name()
	size := len(*f) + len(name)

	if d.IsDir() {
		size++
	}

	c := newFilePathSize(size)

	c.writeBytes(*f)
	c.writeString(name)

	if d.IsDir() {
		c.writeString("/")
	}

	return c
}

func (f *filePath) Bytes() []byte {
	return *f
}

func (f *filePath) String() string {
	return unsafe.String(&(*f)[0], len(*f))
}

// Dirent represents a file system directory entry (a file or a directory),
// providing information about the entry's path, type and inode.
type Dirent struct {
	// Path is the complete path to the directory entry (including both
	// directory and basename)
	Path *filePath

	// Type is the type bits of the file mode of this entry.
	Type os.FileMode

	// Inode is the file system inode number for this entry.
	Inode uint64
}

// newDirentForDirectoryPath returns a Dirent for the given directory, with
// a Type for directories and no Inode.
func newDirentForDirectoryPath(dir string) Dirent {
	return Dirent{Path: newFilePath(dir), Type: fs.ModeDir}
}

// IsDir returns true if we are a directory.
func (d *Dirent) IsDir() bool {
	return d.Type.IsDir()
}

// IsRegular returns true if we are a regular file.
func (d *Dirent) IsRegular() bool {
	return d.Type.IsRegular()
}

// IsSymlink returns true if we are a symlink.
func (d *Dirent) IsSymlink() bool {
	return d.Type&os.ModeSymlink != 0
}
