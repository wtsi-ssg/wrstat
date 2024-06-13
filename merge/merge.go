/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * Author: Daniel Elia <de7@sanger.ac.uk>
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

package merge

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/wtsi-ssg/wrstat/v4/basedirs"
	"github.com/wtsi-ssg/wrstat/v4/neaten"
	"github.com/wtsi-ssg/wrstat/v4/wait"
)

const (
	mergeArgs             = 2
	mergeDatePrefixLength = 8
	mergeMaxWait          = 23 * time.Hour
	reloadGrace           = 15 * time.Minute

	dgutDBsSuffix           = "dgut.dbs"
	dgutDBsSentinelBasename = ".dgut.dbs.updated"
	basedirBasename         = "basedirs.db"
	mergingSuffix           = ".merging"
)

var (
	errDirectoryInvalid = errors.New("one or both directories are invalid")
	errWaitDgut         = errors.New("wait for matching dgut.db outputs failed")
	errMergeDgut        = errors.New("merge of dgut.db directories failed")
	errWaitBasedirs     = errors.New("wait for matching basedirs outputs failed")
	errMergeBasedirs    = errors.New("merge of basedir.dbs failed")
	errWaitDgutDbs      = errors.New("waiting for the dgutdbs sentintal file failed")
	errRenameBasedirs   = errors.New("failed to move the merged basedirs.db file back over original")
	errTouchDgutDbs     = errors.New("failed to touch the dgutdbs sentinal file")
	errDeleteSource     = errors.New("failed to delete source files")
)

type Warning struct {
	error
}

// MergeDBs merges the wrstat databases in the source and dest directories. The second
// value it returns is a 0 if no errors, a 1 if theres an error, and 2 if a warning.
func MergeDB(sourceDir, destDir string, mergeDelete bool) error {
	err := areValidDirectories(sourceDir, destDir)
	if err != nil {
		return fmt.Errorf("%w: %w", errDirectoryInvalid, err)
	}
	fmt.Println("Valid directories")

	sourceDGUTDir, destDGUTDir, err := wait.ForMatchingPrefixOfLatestSuffix(
		dgutDBsSuffix, mergeDatePrefixLength, sourceDir, destDir, mergeMaxWait)
	if err != nil {
		return fmt.Errorf("%w: %w", errWaitDgut, err)
	}
	fmt.Println("Matching dgut.db files found")

	err = neaten.MergeDGUTDBDirectories(sourceDGUTDir, destDGUTDir)
	if err != nil {
		return fmt.Errorf("%w: %w", errMergeDgut, err)
	}
	fmt.Println("Merged dgut.db files")

	sourceBasedir, destBasedir, err := wait.ForMatchingPrefixOfLatestSuffix(
		basedirBasename, mergeDatePrefixLength, sourceDir, destDir, mergeMaxWait)
	if err != nil {
		return fmt.Errorf("%w: %w", errWaitBasedirs, err)
	}
	fmt.Println("Matching basedirs.db files found")

	outputDBPath := destBasedir + mergingSuffix

	err = basedirs.MergeDBs(sourceBasedir, destBasedir, outputDBPath)
	if err != nil {
		return fmt.Errorf("%w: %w", errMergeBasedirs, err)
	}
	fmt.Println("Merged basedirs.db files")

	sentinal := filepath.Join(destDir, dgutDBsSentinelBasename)

	err = wait.UntilFileIsOld(sentinal, reloadGrace)
	if err != nil {
		return fmt.Errorf("%w: %w", errWaitDgutDbs, err)
	}
	fmt.Println("Waited for dgut.dbs sentinal file")

	err = os.Rename(outputDBPath, destBasedir)
	if err != nil {
		return fmt.Errorf("%w: %w", errRenameBasedirs, err)
	}
	fmt.Println("Renamed merged basedirs.db file")

	err = neaten.Touch(sentinal)
	if err != nil {
		return fmt.Errorf("%w: %w", errTouchDgutDbs, err)
	}
	fmt.Println("Touched dgut.dbs sentinal file")

	if mergeDelete {
		err = neaten.DeleteAllPrefixedDirEntries(sourceDir, filepath.Base(sourceBasedir)[:mergeDatePrefixLength])
		if err != nil {
			return Warning{fmt.Errorf("%w: %w", errDeleteSource, err)}
		}
		fmt.Println("Deleted source files")
	}

	fmt.Println("Merge successful")

	return nil
}

// areValidDirectories returns an error if any of the directories fail to stat.
// It returns the first error it encounters, so if a later directory
// theoretically should return an error, you won't see it.
func areValidDirectories(dirs ...string) error {
	for _, dir := range dirs {
		_, err := os.Stat(dir)
		if err != nil {
			return err
		}
	}

	return nil
}
