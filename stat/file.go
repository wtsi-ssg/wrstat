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
	"encoding/base64"
	"fmt"
	io "io/fs"
	"os"
	"path/filepath"
	"syscall"
)

type FileType string

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
	Base64Path string
	Size       int64
	UID        uint32
	GID        uint32
	Atim       int64
	Mtim       int64
	Ctim       int64
	Type       FileType
	Ino        uint64
	Nlink      uint64
	Dev        uint64
}

// ToString produces our special format for describing the stats of a file. It
// is \n terminated and ready to be written to a file.
func (fs *FileStats) ToString() string {
	return fmt.Sprintf(
		"%s\t%d\t%d\t%d\t%d\t%d\t%d\t%s\t%d\t%d\t%d\n",
		fs.Base64Path, fs.Size, fs.UID, fs.GID,
		fs.Atim, fs.Mtim, fs.Ctim,
		fs.Type, fs.Ino, fs.Nlink, fs.Dev)
}

// correctSize will adjust our Size to stat.Blocks*stat.Blksize if our current
// Size is greater than that, to account for files with holes in them.
func (fs *FileStats) correctSize(stat *syscall.Stat_t) {
	if fs.Size > stat.Blocks*stat.Blksize {
		fs.Size = stat.Blocks * stat.Blksize
	}
}

// File interprets the given file info to produce a FileStats.
//
// You provide the directory path the file is in so that Base64Path can be
// calculated for the full file path.
func File(dir string, info os.FileInfo) *FileStats {
	fs := &FileStats{
		Base64Path: base64Encode(filepath.Join(dir, info.Name())),
		Size:       info.Size(),
		Type:       modeToType(info.Mode()),
	}

	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		fs.UID = stat.Uid
		fs.GID = stat.Gid
		fs.Atim = stat.Atim.Sec
		fs.Mtim = stat.Mtim.Sec
		fs.Ctim = stat.Ctim.Sec
		fs.Ino = stat.Ino
		fs.Nlink = stat.Nlink
		fs.Dev = stat.Dev

		fs.correctSize(stat)
	}

	return fs
}

// base64Encode encodes the given string in base64.
func base64Encode(val string) string {
	return base64.StdEncoding.EncodeToString([]byte(val))
}

// modeToType turns a FileMode retrieved from a FileInfo into one of our
// FileType constants.
func modeToType(mode io.FileMode) FileType {
	fileMode := mode.Type()
	if fileMode.IsRegular() {
		return FileTypeRegular
	}

	return nonRegularTypeToFileType(fileMode)
}

// nonRegularTypeToFileType turns a FileMode from FileMode.Type() into one of
// our FileType constants.
func nonRegularTypeToFileType(fileMode io.FileMode) FileType {
	switch fileMode {
	case io.ModeDir:
		return FileTypeDir
	case io.ModeSymlink:
		return FileTypeLink
	case io.ModeSocket:
		return FileTypeSocket
	case io.ModeDevice:
		return FileTypeBlock
	case io.ModeCharDevice:
		return FileTypeChar
	case io.ModeNamedPipe:
		return FileTypeFIFO
	default:
		return FileTypeUnknown
	}
}
