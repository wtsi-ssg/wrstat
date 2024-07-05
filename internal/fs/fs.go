/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
 *
 * Authors:
 *	- Sendu Bala <sb10@sanger.ac.uk>
 *	- Michael Woolnough <mw31@sanger.ac.uk>
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

package fs

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	gas "github.com/wtsi-hgi/go-authserver"
)

const ErrNoDirEntryFound = gas.Error("file not found in directory")

// FindLatestDirectoryEntry finds the latest entry in dir that has the given
// suffix and returns its path.
func FindLatestDirectoryEntry(dir, suffix string) (string, error) {
	des, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}

	sort.Slice(des, func(i, j int) bool {
		return DirEntryModTime(des[i]).After(DirEntryModTime(des[j]))
	})

	for _, de := range des {
		if strings.HasSuffix(de.Name(), "."+suffix) {
			return filepath.Join(dir, de.Name()), nil
		}
	}

	return "", ErrNoDirEntryFound
}

// DirEntryModTime returns the ModTime of the given DirEntry, treating errors as
// time 0.
func DirEntryModTime(de os.DirEntry) time.Time {
	info, err := de.Info()
	if err != nil {
		return time.Time{}
	}

	return info.ModTime()
}

// ModTime returns the ModTime for the given path, treating errors as time 0.
func ModTime(path string) time.Time {
	fi, err := os.Lstat(path)
	if err != nil {
		return time.Time{}
	}

	return fi.ModTime()
}

// Touch updates the modtime and access time of the specified path to the
// specified time.
func Touch(path string, t time.Time) error {
	return os.Chtimes(path, t, t)
}

type pathTime struct {
	path    string
	modtime time.Time
}

// FindLatestCombinedOutputOlderThan finds the latest entry in dir, waits for it
// to be the given age, then returns its path.
func FindLatestCombinedOutputOlderThan(dir, watchFile string, minAge time.Duration) (string, error) {
	for {
		files, err := filepath.Glob(filepath.Join(dir, "*", "*", "*", watchFile))
		if err != nil {
			return "", err
		}

		if len(files) == 0 {
			return "", ErrNoDirEntryFound
		}

		de, err := filesToLatestPathTime(files)
		if err != nil {
			return "", err
		}

		diff := de.modtime.Sub(time.Now().Add(-minAge))

		if diff < 0 {
			return filepath.Dir(filepath.Dir(filepath.Dir(de.path))), nil
		}

		time.Sleep(diff)
	}
}

func filesToLatestPathTime(files []string) (pathTime, error) {
	des := make([]pathTime, len(files))

	for n, file := range files {
		fi, err := os.Lstat(file)
		if err != nil {
			return pathTime{}, err
		}

		des[n] = pathTime{
			path:    file,
			modtime: fi.ModTime(),
		}
	}

	sort.Slice(des, func(i, j int) bool {
		return des[i].modtime.After(des[j].modtime)
	})

	return des[0], nil
}
