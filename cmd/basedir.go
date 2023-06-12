/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
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
	"path/filepath"
	"regexp"
	"time"

	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/v4/basedirs"
	"github.com/wtsi-ssg/wrstat/v4/dgut"
)

const (
	basedirBasename        = "base.dirs"
	basedirSplits          = 4
	basedirMinDirs         = 4
	basedirMinDirsHumgen   = basedirMinDirs + 1
	basedirMinDirsMDTExtra = 1
)

// options for this cmd.
var quotaPath string
var basedirMDTRegexp = regexp.MustCompile(`\/mdt\d(\/|\z)`)
var basedirHumgenRegexp = regexp.MustCompile(`\/lustre\/scratch\d\d\d\/(humgen|hgi|tol|pam|opentargets)`)

// basedirCmd represents the basedir command.
var basedirCmd = &cobra.Command{
	Use:   "basedir",
	Short: "Create a database that summarises disk usage by unix group and base directory.",
	Long: `Create a database that summarises disk usage by unix group and base directory.

Provide the unique subdirectory of your 'wrstat multi -w' directory as an unamed
argument to this command. You must also provide a csv file of group,disk,
size_quota,inode_quota via the --quota option (where size_quota is the maximum
disk usage allowed for that group on that disk in bytes, and inode_quota is the
maximum number of files they can have).

This is called by 'wrstat multi' after the combine step has completed. It does
some 'wrstat where'-type calls for every unix group to come up with hopefully
meaningful and useful "base directories" for every group and ever user.

Unlike the real 'wrstat where', this is not restricted by authorization and
directly accesses the database files to see all data.

A base directory is a directory where all a group/user's data lies nested
within.

Since a group/user could have files in multiple mount points mounted at /, the
true base directory would likely always be '/', which wouldn't be useful.
Instead, a 'wrstat where' split of 4 is used, and only paths consisting of at
least 4 sub directories are returned. Paths that are subdirectories of other
results are ignored. As a special case, if a path contains 'mdt[n]' as a
directory, where n is a number, then 5 sub directories are required.

Disk usage summaries are stored in database files keyed on the group/user and
base directories. The summaries include quota information for groups, taking
that information from the given --quota file. Eg. if the csv has the line:
foo,/mount/a,1024
Then the summary of group foo's data in a base directory /mount/a/groups/foo
would say the quota for that location was 1KB.

The output directory has the name 'basedir.db' in the given directory. If it
already exists with database files inside, those database files are updated with
the latest summary information.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			die("you must supply the path to your unique subdir of your 'wrstat multi -w' working directory")
		}

		if quotaPath == "" {
			die("you must supply --quota")
		}

		quotas, err := basedirs.ParseQuotas(quotaPath)
		if err != nil {
			die("failed to parse quota information: %s", err)
		}

		t := time.Now()
		tree, err := dgut.NewTree(dgutDBCombinePaths(args[0])...)
		if err != nil {
			die("failed to load dgut databases: %s", err)
		}
		info("opening databases took %s", time.Since(t))

		dbDir := filepath.Join(args[0], basedirBasename)

		bd := basedirs.NewCreator(dbDir, tree, quotas)

		t = time.Now()
		err = bd.CreateDatabase()
		if err != nil {
			die("failed to create base directories database: %s", err)
		}
		info("creating base dirs took %s", time.Since(t))
	},
}

func init() {
	RootCmd.AddCommand(basedirCmd)

	// flags specific to this sub-command
	basedirCmd.Flags().StringVarP(&quotaPath, "quota", "q", "", "group,disk,size_quota,inode_quota csv file")
}

// dgutDBCombinePaths returns the dgut db directories that 'wrstat combine'
// creates in the given output directory.
func dgutDBCombinePaths(dir string) []string {
	paths, err := filepath.Glob(fmt.Sprintf("%s/*/*/%s", dir, combineDGUTOutputFileBasename))
	if err != nil || len(paths) == 0 {
		die("failed to find dgut database directories based on [%s/*/*/%s] (err: %s)",
			dir, combineDGUTOutputFileBasename, err)
	}

	info("%+v", paths)

	return paths
}
