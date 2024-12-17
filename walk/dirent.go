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
	"io/fs"
	"os"
	"strings"
)

// FilePath is a byte-slice of a path.
type FilePath struct {
	parent *FilePath
	name   string
	depth  uint16
}

// NewFilePath creates a new FilePath, setting the value to the given string.
func NewFilePath(path string) FilePath {
	return FilePath{name: path}
}

func (f *FilePath) appendTo(p []byte) []byte {
	if f.parent != nil {
		p = f.parent.appendTo(p)
	}

	return append(p, f.name...)
}

// Bytes returns the FilePath as a literal byte-slice.
func (f *FilePath) Bytes() []byte {
	return f.appendTo(nil)
}

func (f *FilePath) compare(g *FilePath) int {
	if f.depth < g.depth {
		return f.compareTo(g.getDepth(f.depth))
	} else if f.depth > g.depth {
		return f.getDepth(g.depth).compareTo(g)
	}

	return f.compareTo(g)
}

func (f *FilePath) getDepth(n uint16) *FilePath {
	for f.depth != n {
		f = f.parent
	}

	return f
}

func (f *FilePath) compareTo(g *FilePath) int {
	if f == g {
		return 0
	}

	cmp := f.parent.compareTo(g.parent)

	if cmp == 0 {
		return strings.Compare(f.name, g.name)
	}

	return cmp
}

// Dirent represents a file system directory entry (a file or a directory),
// providing information about the entry's path, type and inode.
type Dirent struct {
	// Path is the complete path to the directory entry (including both
	// directory and basename)
	Path FilePath

	// Type is the type bits of the file mode of this entry.
	Type fs.FileMode

	// Inode is the file system inode number for this entry.
	Inode uint64
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
