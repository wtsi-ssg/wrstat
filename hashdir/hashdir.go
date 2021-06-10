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

// package hashdir provides a way to create hashed directory structures to
// distribute file outputs evenly on a filesystem.

package hashdir

import (
	"os"
	"path/filepath"
	"strings"
)

const RecommendedLevels = 4

// DirHasher lets you turn strings into hashed directory structures.
type DirHasher struct {
	levels int
}

// New returns a DirHasher that will hash strings in to the given number of
// sub directory levels. A good number to use is hashdir.RecommendedLevels.
func New(levels int) *DirHasher {
	return &DirHasher{levels: levels}
}

// Hash hashes the given value and returns it split in to our number of levels.
func (h *DirHasher) Hash(value string) []string {
	return strings.SplitN(ByteKey([]byte(value)), "", h.levels)
}

// HashDir uses Hash() on the given value and returns a directory path rooted
// at the given baseDir consisting of the Hash() result, with the last value
// returned seperatly (that you could use as the leaf name of a file).
//
// If baseDir is not absolute, it will be made absolute relative to the current
// working directory. (Trying to do that may return an error.)
func (h *DirHasher) HashDir(baseDir, value string) (string, string, error) {
	dirs := h.Hash(value)
	dirs, leaf := dirs[0:h.levels-1], dirs[h.levels-1]

	abs, err := filepath.Abs(baseDir)
	if err != nil {
		return "", "", err
	}

	dirs = append([]string{abs}, dirs...)

	return filepath.Join(dirs...), leaf, nil
}

// MkDirHashed uses HashDir() on the inputs, creates the returned
// subdirectories, creates a file named after the leaf, opens it writing and
// returns it.
func (h *DirHasher) MkDirHashed(baseDir, value string) (*os.File, error) {
	dirs, leaf, err := h.HashDir(baseDir, value)
	if err != nil {
		return nil, err
	}

	err = os.MkdirAll(dirs, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return os.Create(filepath.Join(dirs, leaf))
}
