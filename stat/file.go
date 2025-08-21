/*******************************************************************************
 * Copyright (c) 2021 Genome Research Ltd.
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

// package stat lets you get stats on files and directories in a certain format.

package stat

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"syscall"
)

type FileType string

// blockSize is the number of bytes in a block of st_blocks. st_blksize is
// unrelated.
// See http://www.gnu.org/software/libc/manual/html_node/Attribute-Meanings.html
const blockSize = 512

const (
	FileTypeRegular FileType = "f"
	FileTypeLink    FileType = "l"
	FileTypeDir     FileType = "d"
	FileTypeSocket  FileType = "s"
	FileTypeBlock   FileType = "b"
	FileTypeChar    FileType = "c"
	FileTypeFIFO    FileType = "F"
	FileTypeUnknown FileType = "X"
)

// FileStats contains all the file stats needed by wrstat, interpreted in our
// custom way.
type FileStats struct {
	Path         string
	Size         int64
	ApparentSize int64
	UID          uint32
	GID          uint32
	Atim         int64
	Mtim         int64
	Ctim         int64
	Type         FileType
	Ino          uint64
	Nlink        uint64
	Dev          uint64
}

// WriteTo produces our special format for describing the stats of a file. It
// is \n terminated and writes to the given Writer.
func (fs *FileStats) WriteTo(w io.Writer) (int64, error) {
	n, err := fmt.Fprintf(w,
		"%q\t%d\t%d\t%d\t%d\t%d\t%d\t%s\t%d\t%d\t%d\t%d\n",
		fs.Path, fs.Size, fs.UID, fs.GID,
		fs.Atim, fs.Mtim, fs.Ctim,
		fs.Type, fs.Ino, fs.Nlink, fs.Dev, fs.ApparentSize)

	return int64(n), err
}

// File interprets the given file info to produce a FileStats.
//
// You provide the absolute path to the file so that QuotedPath can be
// calculated correctly (the info only contains the basename).
func File(absPath string, info os.FileInfo, statBlockSize bool) FileStats {
	fs := FileStats{
		Path:         absPath,
		Size:         info.Size(),
		ApparentSize: info.Size(),
		Type:         modeToType(info.Mode()),
	}

	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		fs.UID = stat.Uid
		fs.GID = stat.Gid
		fs.Atim = stat.Atim.Sec
		fs.Mtim = stat.Mtim.Sec
		fs.Ctim = stat.Ctim.Sec
		fs.Ino = stat.Ino
		fs.Nlink = uint64(stat.Nlink) //nolint:unconvert
		fs.Dev = stat.Dev

		if statBlockSize {
			fs.Size = stat.Blocks * blockSize
		}
	}

	return fs
}

// modeToType turns a FileMode retrieved from a FileInfo into one of our
// FileType constants.
func modeToType(mode fs.FileMode) FileType {
	fileMode := mode.Type()
	if fileMode.IsRegular() {
		return FileTypeRegular
	}

	return nonRegularTypeToFileType(fileMode)
}

// nonRegularTypeToFileType turns a FileMode from FileMode.Type() into one of
// our FileType constants.
func nonRegularTypeToFileType(fileMode fs.FileMode) FileType {
	switch fileMode {
	case fs.ModeDir:
		return FileTypeDir
	case fs.ModeSymlink:
		return FileTypeLink
	case fs.ModeSocket:
		return FileTypeSocket
	case fs.ModeDevice:
		return FileTypeBlock
	case fs.ModeCharDevice:
		return FileTypeChar
	case fs.ModeNamedPipe:
		return FileTypeFIFO
	default:
		return FileTypeUnknown
	}
}

// FileOperation returns an Operation that can be used with Paths that calls
// File() on each path the Operation receives and outputs the ToString() value
// to the given output file.
//
// statBlockSize set to true will cause the primary size column to be the
// on-disk block size instead of the apparent byte size of the file. Regardless
// of the value, the apparent size is also stored in a later column.
//
// logWrites is a function that will be called on each write to an output put
// with the number of bytes written.
func FileOperation(output io.Writer, statBlockSize bool, logWrites func(int64)) Operation {
	return func(path string, info fs.FileInfo) error {
		f := File(path, info, statBlockSize)
		b, errw := f.WriteTo(output)

		if logWrites != nil {
			logWrites(b)
		}

		return errw
	}
}
