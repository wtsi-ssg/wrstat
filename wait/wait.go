/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * Author: Ash Holland <ah37@sanger.ac.uk>
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

// package wait is

package wait

import (
	"context"
	"os"
	"path/filepath"
	"time"

	bs "github.com/wtsi-ssg/wr/backoff/time"
	"github.com/wtsi-ssg/wr/retry"
	"github.com/wtsi-ssg/wrstat/v4/internal/fs"
)

type Error string

func (e Error) Error() string { return string(e) }

const ErrNoMatchingFiles = Error("There are no matching files according to the provided suffix.")

// ForMatchingPrefixOfLatestSuffix waits the given timeLimit for both sourceDir
// and destDir to contain a file or directory with the given suffix that are
// both the most recent entry with the suffix in their respective dirs, and have
// the same prefix of the given length.
func ForMatchingPrefixOfLatestSuffix(suffix string, prefixLen int,
	sourceDir, destDir string, timeLimit time.Duration) (string, string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeLimit)
	defer cancel()

	var (
		latestSource, latestDest string
		err                      error
	)

	op := func() error {
		latestSource, latestDest, err = getLatestSuffixesWithMathchingPrefix(suffix, prefixLen, sourceDir, destDir)

		return err
	}

	status := retry.Do(ctx, op, &retry.UntilNoError{}, bs.SecondsRangeBackoff(), "Waiting for prefix match")
	if status.Err != nil {
		return "", "", err
	}

	return latestSource, latestDest, nil
}

func getLatestSuffixesWithMathchingPrefix(suffix string, prefixLen int,
	sourceDir, destDir string) (string, string, error) {
	latestSource, err := fs.FindLatestDirectoryEntry(sourceDir, suffix)
	if err != nil {
		return "", "", err
	}

	latestDest, err := fs.FindLatestDirectoryEntry(destDir, suffix)
	if err != nil {
		return "", "", err
	}

	latestSourcePrefix := filepath.Base(latestSource)[:prefixLen]
	latestDestPrefix := filepath.Base(latestDest)[:prefixLen]

	if latestSourcePrefix != latestDestPrefix {
		return "", "", ErrNoMatchingFiles
	}

	return latestSource, latestDest, nil
}

// UntilFileIsOld waits for the file at the given path to be least age old.
//
// The file's "age" is time since it's mtime. If the file is touched while we
// are waiting, we will wait more time.
//
// Returns an error if the file doesn't exist.
func UntilFileIsOld(path string, age time.Duration) error {
	for {
		info, err := os.Stat(path)
		if err != nil {
			return err
		}

		wait := time.Until(info.ModTime().Add(age))
		if wait <= 0*time.Second {
			break
		}

		<-time.After(wait)
	}

	return nil
}
