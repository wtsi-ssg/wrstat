/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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

package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/dgut"
)

const (
	basedirBasename   = "base.dirs"
	basedirSplits     = 4
	basedirMinDirs    = 4
	basedirMinDirsMDT = 5
)

var basedirMDTRegexp = regexp.MustCompile(`\/mdt\d\/`)

// basedirCmd represents the basedir command.
var basedirCmd = &cobra.Command{
	Use:   "basedir",
	Short: "Calculate base directories for every unix group.",
	Long: `Calculate base directories for every unix group.

Provide your 'wrstat multi -f' argument as an unamed argument to this command.

This is called by 'wrstat multi' after the tidy step has completed. It does some
'wrstat where'-type calls for every unix group to come up with hopefully
meaningful and useful "base directories" for every group.

Unlike the real 'wrstat where', this is not restricuted by authorization and
directly accesses the database files to see all data.

A base directory is a directory where all a group's data lies nested within.

Since a group could have files in multiple mount points mounted at /, the true
base directory would likely always be '/', which wouldn't be useful. Instead,
a 'wrstat where' split of 4 is used, and only paths consisting of at least 4
sub directories are returned. Paths that are subdirectories of other results are
ignored. As a special case, if a path contains 'mdt[n]' as a directory, where n
is a number, then 5 sub directories are required.

The output file format is 2 tab separated columns with the following contents:
1. Unix group ID.
2. Absolute path of base directory.

The output file has the fixed name 'base.dirs' in the given directory, and will
overwrite any such file already there.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			die("you must supply the path to your 'wrstat multi -f' output directory")
		}

		t := time.Now()
		tree, err := dgut.NewTree(dgutDBPaths(args[0])...)
		if err != nil {
			die("failed to load dgut databases: %s", err)
		}
		info("opening databases took %s", time.Since(t))

		t = time.Now()
		gids, err := getAllGIDsInTree(tree)
		if err != nil {
			die("failed to get all unix groups: %s", err)
		}
		info("getting GIDs took %s", time.Since(t))

		t = time.Now()
		err = calculateBaseDirs(tree, filepath.Join(args[0], basedirBasename), gids)
		if err != nil {
			die("failed to create base.dirs: %s", err)
		}
		info("calculating base dirs took %s", time.Since(t))
	},
}

func init() {
	RootCmd.AddCommand(basedirCmd)
}

// getAllGIDsInTree gets all the unix group IDs that own files in the given file
// tree.
func getAllGIDsInTree(tree *dgut.Tree) ([]uint32, error) {
	di, err := tree.DirInfo("/", nil)
	if err != nil {
		return nil, err
	}

	return di.Current.GIDs, nil
}

// calculateBaseDirs does the main work of this command.
func calculateBaseDirs(tree *dgut.Tree, outPath string, gids []uint32) error {
	outFile, err := os.Create(outPath)
	if err != nil {
		return err
	}

	for _, gid := range gids {
		baseDirs, errc := calculateBaseDirsOfGID(tree, gid)
		if errc != nil {
			return errc
		}

		if errw := writeBaseDirsOfGID(outFile, gid, baseDirs); errw != nil {
			return errw
		}
	}

	return nil
}

// calculateBaseDirsOfGID uses the tree to work out what the base directories of
// the given GID are.
func calculateBaseDirsOfGID(tree *dgut.Tree, gid uint32) ([]string, error) {
	dcss, err := tree.Where("/", &dgut.Filter{GIDs: []uint32{gid}}, basedirSplits)
	if err != nil {
		return nil, err
	}

	sort.Slice(dcss, func(i, j int) bool {
		return dcss[i].Dir < dcss[j].Dir
	})

	var dirs []string //nolint:prealloc

	var previous string

	for _, ds := range dcss {
		if notEnoughDirs(ds.Dir) || childOfPreviousResult(ds.Dir, previous) {
			continue
		}

		dirs = append(dirs, ds.Dir)
		previous = ds.Dir
	}

	return dirs, nil
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

// writeBaseDirsOfGID writes entries to the output file for the given gid and
// its base directories.
func writeBaseDirsOfGID(outFile *os.File, gid uint32, dirs []string) error {
	for _, dir := range dirs {
		if _, err := outFile.WriteString(fmt.Sprintf("%d\t%s\n", gid, dir)); err != nil {
			return err
		}
	}

	return nil
}
