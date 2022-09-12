/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * Partially based on github.com/MichaelTJones/walk
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

package basedirs

import (
	"regexp"
	"strings"

	"github.com/wtsi-ssg/wrstat/v4/dgut"
)

const (
	basedirSplits     = 4
	basedirMinDirs    = 4
	basedirMinDirsMDT = 5
)

var basedirMDTRegexp = regexp.MustCompile(`\/mdt\d(\/|\z)`)

// getAllGIDsandUIDsInTree gets all the unix group and user IDs that own files
// in the given file tree.
func getAllGIDsandUIDsInTree(tree *dgut.Tree) ([]uint32, []uint32, error) {
	di, err := tree.DirInfo("/", nil)
	if err != nil {
		return nil, nil, err
	}

	return di.Current.GIDs, di.Current.UIDs, nil
}

// summariseBaseDirs stores summary disk usage information in databases in the
// given dir, keyed on id:basedir. Pass in quotas information if working on GIDs
// to store quota values in the summaries.
func summariseBaseDirs(tree *dgut.Tree, dir string, ids []uint32, q *Quotas) error {
	for _, id := range ids {
		err := summariseBaseDirsOfID(tree, id, q)
		if err != nil {
			return err
		}
	}

	return nil
}

// summariseBaseDirsOfID uses the tree to work out what the base directories of
// the given ID are. Then summarises disk usage for the id:basedir pair, storing
// info in the db. Pass a non-null quotas to treat the id as a gid; otherwise it
// will be considered a uid.
//
// We manipulate Where() results instead of using FileLocations(), because
// empirically that is too noisy.
func summariseBaseDirsOfID(tree *dgut.Tree, id uint32, q *Quotas) error {
	filter := &dgut.Filter{GIDs: []uint32{id}}
	if q == nil {
		filter = &dgut.Filter{UIDs: []uint32{id}}
	}

	dcss, err := tree.Where("/", filter, basedirSplits)
	if err != nil {
		return err
	}

	dcss.SortByDir()

	var previous string

	for _, ds := range dcss {
		if notEnoughDirs(ds.Dir) || childOfPreviousResult(ds.Dir, previous) {
			continue
		}

		storeSummariesInDB(ds, id, q)

		previous = ds.Dir
	}

	return nil
}

// notEnoughDirs returns true if the given path has fewer than 4 directories.
// If path has an mdt directory in it, then it becomes 5 directories.
func notEnoughDirs(path string) bool {
	numDirs := strings.Count(path, "/")

	min := basedirMinDirs
	if basedirMDTRegexp.MatchString(path) {
		min = basedirMinDirsMDT
	}

	return numDirs < min
}

// childOfPreviousResult returns true if previous is not blank, and dir starts
// with it.
func childOfPreviousResult(dir, previous string) bool {
	return previous != "" && strings.HasPrefix(dir, previous)
}
