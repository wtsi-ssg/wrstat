/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
 *   Michael Woolnough <mw31@sanger.ac.uk>
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

// package basedirs is used to summarise disk usage information by base
// directory, storing and retrieving the information from an embedded database.

package basedirs

import (
	"regexp"
	"strings"

	"github.com/ugorji/go/codec"
	"github.com/wtsi-ssg/wrstat/v4/dgut"
	bolt "go.etcd.io/bbolt"
)

const (
	basedirSplits     = 4
	basedirMinDirs    = 4
	basedirMinDirsMDT = 5
)

var basedirMDTRegexp = regexp.MustCompile(`\/mdt\d(\/|\z)`)

// BaseDirs is used to summarise disk usage information by base directory and
// group or user.
type BaseDirs struct {
	dir         string
	tree        *dgut.Tree
	quotas      *Quotas
	ch          codec.Handle
	mountPoints mountPoints
}

// NewCreator returns a BaseDirs that lets you create a database summarising
// usage information by base directory, taken from the given tree and quotas.
func NewCreator(dir string, tree *dgut.Tree, quotas *Quotas) (*BaseDirs, error) {
	mp, err := getMountPoints()
	if err != nil {
		return nil, err
	}

	return &BaseDirs{
		dir:         dir,
		tree:        tree,
		quotas:      quotas,
		ch:          new(codec.BincHandle),
		mountPoints: mp,
	}, nil
}

// CalculateForGroup calculates all the base directories for the given group.
func (b *BaseDirs) CalculateForGroup(gid uint32) (dgut.DCSs, error) {
	var dcss dgut.DCSs

	if err := b.filterWhereResults(&dgut.Filter{GIDs: []uint32{gid}}, func(ds *dgut.DirSummary) {
		dcss = append(dcss, ds)
	}); err != nil {
		return nil, err
	}

	return dcss, nil
}

func (b *BaseDirs) filterWhereResults(filter *dgut.Filter, cb func(ds *dgut.DirSummary)) error {
	dcss, err := b.tree.Where("/", filter, basedirSplits)
	if err != nil {
		return err
	}

	dcss.SortByDir()

	var previous string

	for _, ds := range dcss {
		if notEnoughDirs(ds.Dir) || childOfPreviousResult(ds.Dir, previous) {
			continue
		}

		cb(ds)

		// used to be `dirs = append(dirs, ds.Dir)`
		// then for each dir, `outFile.WriteString(fmt.Sprintf("%d\t%s\n", gid, dir))`

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

// CalculateForUser calculates all the base directories for the given user.
func (b *BaseDirs) CalculateForUser(uid uint32) (dgut.DCSs, error) {
	var dcss dgut.DCSs

	if err := b.filterWhereResults(&dgut.Filter{UIDs: []uint32{uid}}, func(ds *dgut.DirSummary) {
		dcss = append(dcss, ds)
	}); err != nil {
		return nil, err
	}

	return dcss, nil
}

// BaseDirReader is used to read the information stored in a BaseDir database.
type BaseDirReader struct {
	db          *bolt.DB
	ch          codec.Handle
	mountPoints mountPoints
}

// NewReader returns a BaseDirReader that can return the summary information
// stored in a BaseDir database.
func NewReader(path string) (*BaseDirReader, error) {
	db, err := bolt.Open(path, dbOpenMode, &bolt.Options{
		ReadOnly: true,
	})
	if err != nil {
		return nil, err
	}

	mp, err := getMountPoints()
	if err != nil {
		return nil, err
	}

	return &BaseDirReader{
		db:          db,
		ch:          new(codec.BincHandle),
		mountPoints: mp,
	}, nil
}

func (b *BaseDirReader) Close() error {
	return b.db.Close()
}
