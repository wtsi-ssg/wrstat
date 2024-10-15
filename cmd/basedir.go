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

package cmd

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/v5/basedirs"
	"github.com/wtsi-ssg/wrstat/v5/dgut"
	ifs "github.com/wtsi-ssg/wrstat/v5/internal/fs"
)

const (
	basedirBasename    = "basedirs.db"
	groupUsageBasename = "basedirs.groupusage.tsv"
	userUsageBasename  = "basedirs.userusage.tsv"
	numBasedirArgs     = 2
	defaultSplits      = 1
	defaultMinDirs     = 2
)

// options for this cmd.
var (
	quotaPath  string
	ownersPath string
	configPath string
)

// basedirCmd represents the basedir command.
var basedirCmd = &cobra.Command{
	Use:   "basedir",
	Short: "Create a database that summarises disk usage by unix group and base directory.",
	Long: `Create a database that summarises disk usage by unix group and base directory.

Provide the unique subdirectory of your 'wrstat multi -w' directory as an unamed
argument to this command, along with the multi -f directory used for the last
time this was run (or the current -f directory for a first run).

You must also provide a csv file of gid,disk,size_quota,inode_quota via the
--quota option (where size_quota is the maximum disk usage allowed for that
group on that disk in bytes, and inode_quota is the maximum number of file they
can have).

You must also provide a csv file of gid,owner_name via the --owners option.

This is called by 'wrstat multi' after the combine step has completed. It does
some 'wrstat where'-type calls for every unix group to come up with hopefully
meaningful and useful "base directories" for every group and ever user.

Unlike the real 'wrstat where', this is not restricted by authorization and
directly accesses the database files to see all data.

A base directory is a directory where all a group/user's data lies nested
within.

Since a group/user could have files in multiple mount points mounted at /, the
true base directory would likely always be '/', which wouldn't be useful. 
Instead, you can provide as TSV based config file, with the following format:

PREFIX	SPLITS	MINDIRS

…where the PREFIX is a path prefix, the SPLITS values matches the usage in the
where subcommand, and the MINDIRS values are the minimum number of nodes a path
must possess. If you don't provide a config file, one like "/ 1 2" is used by
default.

If you expect data specific to different groups to appear 5 directories deep in
different mount points, then a splits value of  4 and mindirs value of 4 might
work well. If you expect it to appear 2 directories deep, the defaults of
splits=1 and mindirs=2 might work well.

Disk usage summaries are stored in database keyed on the group/user and base
directories. The summaries include quota information for groups, taking
that information from the given --quota file. Eg. if the csv has the line:
foo,/mount/a,1024
Then the summary of group foo's data in a base directory /mount/a/groups/foo
would say the quota for that location was 1KB.
The summaries also include the owner of each group, taken from the --ownersfile.

The output is a database named 'basedirs.db' in the given directory. If the file
already exists, that database will be updated with the latest summary
information.

In addition to the database file, it also outputs basedirs.groupusage.tsv which
is a tsv file with these columns:
group_name
owner_name
directory_path
last_modified (number of days ago)
used size (used bytes)
quota size (maximum allowed bytes)
used inodes (number of files)
quota inodes (maximum allowed number of bytes)
warning ("OK" or "Not OK" if quota is estimated to have run out in 3 days)

There's also a similar basedirs.userusage.tsv file with the same columns (but
quota will always be 0, warning will always be "OK", owner_name will always
be blank), and the first column will be user_name instead of group_name.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != numBasedirArgs {
			die("you must supply the path to your unique subdir of your 'wrstat multi -w' working directory, " +
				"and the multi -f output directory")
		}

		if quotaPath == "" {
			die("you must supply --quota")
		}

		if ownersPath == "" {
			die("you must supply --owners")
		}

		quotas, err := basedirs.ParseQuotas(quotaPath)
		if err != nil {
			die("failed to parse quota information: %s", err)
		}

		basedirsConfig := config()

		t := time.Now()
		tree, err := dgut.NewTree(dgutDBCombinePaths(args[0])...)
		if err != nil {
			die("failed to load dgut databases: %s", err)
		}
		info("opening databases took %s", time.Since(t))

		dbPath := filepath.Join(args[0], basedirBasename)

		if err = copyExistingBaseDirsDB(args[1], dbPath); err != nil {
			die("failed to get existing base directories database: %s", err)
		}

		bd, err := basedirs.NewCreator(dbPath, basedirsConfig, tree, quotas)
		if err != nil {
			die("failed to create base directories database: %s", err)
		}

		t = time.Now()
		err = bd.CreateDatabase()
		if err != nil {
			die("failed to create base directories database: %s", err)
		}

		info("creating base dirs took %s", time.Since(t))

		t = time.Now()
		bdr, err := basedirs.NewReader(dbPath, ownersPath)
		if err != nil {
			die("failed to create base directories database: %s", err)
		}

		gut, err := bdr.GroupUsageTable()
		if err != nil {
			die("failed to get group usage table: %s", err)
		}

		if err = writeFile(filepath.Join(args[0], groupUsageBasename), gut); err != nil {
			die("failed to write group usage table: %s", err)
		}

		uut, err := bdr.UserUsageTable()
		if err != nil {
			die("failed to get group usage table: %s", err)
		}

		if err = writeFile(filepath.Join(args[0], userUsageBasename), uut); err != nil {
			die("failed to write group usage table: %s", err)
		}

		if err = bdr.Close(); err != nil {
			die("failed to close basedirs database reader: %s", err)
		}

		info("reading base dirs took %s", time.Since(t))
	},
}

func init() {
	RootCmd.AddCommand(basedirCmd)

	// flags specific to this sub-command
	basedirCmd.Flags().StringVarP(&quotaPath, "quota", "q", "", "gid,disk,size_quota,inode_quota csv file")
	basedirCmd.Flags().StringVarP(&ownersPath, "owners", "o", "", "gid,owner csv file")
	basedirCmd.Flags().StringVarP(&configPath, "config", "b", "", "path to basedirs config file")
}

func config() basedirs.Config {
	if configPath == "" {
		return basedirs.Config{
			basedirs.ConfigAttrs{
				Prefix:  "/",
				Splits:  defaultSplits,
				MinDirs: defaultMinDirs,
			},
		}
	}

	f, err := os.Open(configPath)
	if err != nil {
		die("error opening config: %s", err)
	}

	basedirsConfig, err := basedirs.ParseConfig(f)
	if err != nil {
		die("error parsing basedirs config: %s", err)
	}

	f.Close()

	return basedirsConfig
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

func copyExistingBaseDirsDB(existingDir, newDBPath string) error {
	existingDBPath, err := ifs.FindLatestDirectoryEntry(existingDir, basedirBasename)
	if err != nil && !errors.Is(err, ifs.ErrNoDirEntryFound) {
		return err
	}

	if existingDBPath == "" {
		return nil
	}

	fr, err := os.Open(existingDBPath)
	if err != nil {
		return err
	}

	defer fr.Close()

	fw, err := os.Create(newDBPath)
	if err != nil {
		return err
	}

	_, err = io.Copy(fw, fr)

	errc := fw.Close()
	if err == nil {
		err = errc
	}

	return err
}

func writeFile(path, contents string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}

	if _, err := io.WriteString(f, contents); err != nil {
		return err
	}

	return f.Close()
}
