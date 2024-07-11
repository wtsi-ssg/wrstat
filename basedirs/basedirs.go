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
	"path/filepath"
	"strings"

	"github.com/ugorji/go/codec"
	"github.com/wtsi-ssg/wrstat/v4/dgut"
)

// BaseDirs is used to summarise disk usage information by base directory and
// group or user.
type BaseDirs struct {
	dbPath      string
	config      Config
	tree        *dgut.Tree
	quotas      *Quotas
	ch          codec.Handle
	mountPoints mountPoints
}

// NewCreator returns a BaseDirs that lets you create a database summarising
// usage information by base directory, taken from the given tree and quotas.
//
// Choose splits and minDirs based on how many directories deep you expect data
// for different groups/users to appear. Eg. if your file structure is
// `/mounts/[group name]`, that's 2 directories deep and splits 1, minDirs 2
// might work well. If it's 5 directories deep, splits 4, minDirs 4 might work
// well.
func NewCreator(dbPath string, c Config, tree *dgut.Tree, quotas *Quotas) (*BaseDirs, error) {
	mp, err := getMountPoints()
	if err != nil {
		return nil, err
	}

	return &BaseDirs{
		dbPath:      dbPath,
		config:      c,
		tree:        tree,
		quotas:      quotas,
		ch:          new(codec.BincHandle),
		mountPoints: mp,
	}, nil
}

// SetMountPoints can be used to manually set your mountpoints, if the automatic
// discovery of mountpoints on your system doesn't work.
func (b *BaseDirs) SetMountPoints(mountpoints []string) {
	b.mountPoints = mountpoints
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
	dcss, err := b.tree.Where("/", filter, splitFnFromConfig(b.config))
	if err != nil {
		return err
	}

	dcss.SortByDir()

	var previous string

	for _, ds := range dcss {
		if b.notEnoughDirs(ds.Dir) || childOfPreviousResult(ds.Dir, previous) {
			continue
		}

		cb(ds)

		// used to be `dirs = append(dirs, ds.Dir)`
		// then for each dir, `outFile.WriteString(fmt.Sprintf("%d\t%s\n", gid, dir))`

		previous = ds.Dir
	}

	return nil
}

func splitFnFromConfig(c Config) func(string) int {
	return func(path string) int {
		return int(findBestMatchingConfig(c, path).Splits)
	}
}

func findBestMatchingConfig(c Config, path string) ConfigAttrs {
	var (
		maxScore int
		conf     ConfigAttrs
	)

	for _, p := range c {
		parts := strings.Split(path, "/")
		prefixParts := strings.Split(p.Prefix, "/")

		if len(parts) < len(prefixParts) {
			continue
		}

		var score int

		for i, part := range prefixParts {
			if match, _ := filepath.Match(part, parts[i]); !match {
				score = -1

				break
			}

			if !strings.Contains(part, "*") {
				score++
			} else {

			}
		}

		if score > maxScore {
			maxScore = score
			conf = p
		}
	}

	return conf
}

// notEnoughDirs returns true if the given path has fewer than minDirs
// directories. If path has an mdt directory in it, then it becomes an extra
// directory.
func (b *BaseDirs) notEnoughDirs(path string) bool {
	numDirs := strings.Count(path, "/")

	min := int(findBestMatchingConfig(b.config, path).MinDirs)

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
