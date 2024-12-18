/*******************************************************************************
 * Copyright (c) 2023, 2024 Genome Research Ltd.
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

package walk

import (
	"bytes"
	"io/fs"
	"os"
	"sync"
)

func newDirentPool(size int) *sync.Pool {
	return &sync.Pool{
		New: func() any {
			return &Dirent{
				name: make([]byte, 0, size),
			}
		},
	}
}

var (
	direntPool0   = newDirentPool(0)   //nolint:gochecknoglobals
	direntPool32  = newDirentPool(32)  //nolint:gochecknoglobals,mnd
	direntPool64  = newDirentPool(64)  //nolint:gochecknoglobals,mnd
	direntPool128 = newDirentPool(128) //nolint:gochecknoglobals,mnd
	dirEntPool256 = newDirentPool(257) //nolint:gochecknoglobals,mnd
)

func getDirent(size int) *Dirent {
	switch {
	case size == 0:
		return direntPool0.Get().(*Dirent) //nolint:forcetypeassert
	case size <= 32: //nolint:mnd
		return direntPool32.Get().(*Dirent) //nolint:forcetypeassert
	case size <= 64: //nolint:mnd
		return direntPool64.Get().(*Dirent) //nolint:forcetypeassert
	case size <= 128: //nolint:mnd
		return direntPool128.Get().(*Dirent) //nolint:forcetypeassert
	}

	return dirEntPool256.Get().(*Dirent) //nolint:forcetypeassert
}

func putDirent(d *Dirent) {
	d.name = d.name[:0]
	d.parent = nil

	switch cap(d.name) {
	case 0:
		direntPool0.Put(d)
	case 32: //nolint:mnd
		direntPool32.Put(d)
	case 64: //nolint:mnd
		direntPool64.Put(d)
	case 128: //nolint:mnd
		direntPool128.Put(d)
	default:
		dirEntPool256.Put(d)
	}
}

// Dirent represents a file system directory entry (a file or a directory),
// providing information about the entry's path, type and inode.
type Dirent struct {
	parent *Dirent
	name   []byte
	depth  uint16

	// Type is the type bits of the file mode of this entry.
	Type fs.FileMode

	// Inode is the file system inode number for this entry.
	Inode uint64

	next  *Dirent
	ready sync.Mutex
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

func (d *Dirent) appendTo(p []byte) []byte {
	if d.parent != nil {
		p = d.parent.appendTo(p)
	}

	return append(p, d.name...)
}

// Bytes returns the FilePath as a literal byte-slice.
func (d *Dirent) Bytes() []byte {
	return d.appendTo(nil)
}

func (d *Dirent) compare(e *Dirent) int {
	if d.depth < e.depth {
		e = e.getDepth(d.depth)
	} else if d.depth > e.depth {
		d = d.getDepth(e.depth)
	}

	return e.compareTo(d)
}

func (d *Dirent) getDepth(n uint16) *Dirent {
	for d.depth != n {
		d = d.parent
	}

	return d
}

func (d *Dirent) compareTo(e *Dirent) int {
	if d == e {
		return 0
	}

	cmp := d.parent.compareTo(e.parent)

	if cmp == 0 {
		return bytes.Compare(d.name, e.name)
	}

	return cmp
}

func (d *Dirent) sub(name []byte, mode fs.FileMode, inode uint64) *Dirent {
	de := getDirent(len(name))

	de.parent = d
	de.name = append(de.name, name...)
	de.depth = d.depth + 1
	de.Type = mode
	de.Inode = inode

	if mode.IsDir() {
		de.ready.Lock()
	}

	return de
}

func (d *Dirent) done() *Dirent {
	next := d.next
	d.next = nil

	if len(d.name) == 0 {
		putDirent(d.parent)
	}

	if !d.IsDir() {
		putDirent(d)
	}

	return next
}
