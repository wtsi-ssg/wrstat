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
)

var (
	waitDgutError       = errors.New("wait for matching dgut.db outputs failed")
	mergeDgutError      = errors.New("merge of dgut.db directories failed")
	mergeBasedirsError  = errors.New("merge of basedir.dbs failed")
	waitBasedirsError   = errors.New("wait for matching basedirs outputs failed")
	waitDgutDbsError    = errors.New("waiting for the dgutdbs sentintal file failed")
	renameBasedirsError = errors.New("failed to move the merged basedirs.db file back over original")
	touchDgutDbsError   = errors.New("failed to touch the dgutdbs sentinal file")
	deleteSourceWarning = errors.New("failed to delete source files")
)

// MergeDBs merges the wrstat databases in the source and dest directories. The second
// value it returns is a 0 if no errors, a 1 if theres an error, and 2 if a warning.
func mergeDB(sourceDir, destDir, dgutDBsSuffix, basedirBasename, dgutDBsSentinelBasename string, mergeDelete bool) (error, int) {
	sourceDGUTDir, destDGUTDir, err := wait.ForMatchingPrefixOfLatestSuffix(
		dgutDBsSuffix, mergeDatePrefixLength, sourceDir, destDir, mergeMaxWait)
	if err != nil {
		return fmt.Errorf("%w: %w", waitDgutError, err), 1
	}

	err = neaten.MergeDGUTDBDirectories(sourceDGUTDir, destDGUTDir)
	if err != nil {
		return fmt.Errorf("%w: %w", mergeDgutError, err), 1
	}

	sourceBasedir, destBasedir, err := wait.ForMatchingPrefixOfLatestSuffix(
		basedirBasename, mergeDatePrefixLength, sourceDir, destDir, mergeMaxWait)
	if err != nil {
		return fmt.Errorf("%w: %w", waitBasedirsError, err), 1
	}

	outputDBPath := destBasedir + ".merging"

	err = basedirs.MergeDBs(sourceBasedir, destBasedir, outputDBPath)
	if err != nil {
		return fmt.Errorf("%w: %w", mergeBasedirsError, err), 1
	}

	sentinal := filepath.Join(destDir, dgutDBsSentinelBasename)

	err = wait.UntilFileIsOld(sentinal, reloadGrace)
	if err != nil {
		return fmt.Errorf("%w: %w", waitDgutDbsError, err), 1
	}

	err = os.Rename(outputDBPath, destBasedir)
	if err != nil {
		return fmt.Errorf("%w: %w", renameBasedirsError, err), 1
	}

	err = neaten.Touch(sentinal)
	if err != nil {
		return fmt.Errorf("%w: %w", touchDgutDbsError, err), 1
	}

	if mergeDelete {
		err = neaten.DeleteAllPrefixedDirEntries(sourceDir, filepath.Base(sourceBasedir)[:mergeDatePrefixLength])
		if err != nil {
			return fmt.Errorf("%w: %w", deleteSourceWarning, err), 2
		}
	}

	return nil, 0
}
