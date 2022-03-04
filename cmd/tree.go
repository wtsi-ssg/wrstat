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
	"compress/gzip"
	"fmt"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/dgut"
	"github.com/wtsi-ssg/wrstat/summary"
)

type Error string

func (e Error) Error() string { return string(e) }

const errNoDGUTFilesFound = Error("no combine.dgut.gz files were found")

const bytesPerK = 1024

const dgutDBBasename = "dgut.db"

const dgutStoreBatchSize = 10000

// options for this cmd.
var treeQueryDir string
var treeSplits int
var treeGroups string
var treeUsers string
var treeTypes string
var treeMinimum int

// treeCmd represents the tree command.
var treeCmd = &cobra.Command{
	Use:   "tree",
	Short: "Query a dgut.db that knows where files are",
	Long: `Query a dgut.db that knows where files are in the filesystem tree.

Query an existing database by providing the path to a dgut.db file (as produced
by wrstat tree create), and the --dir you wish to query (defaults to the root
directory).

This tool will show where data really lies: the deepest directory that has all
filter-passing files nested under it.

With a --splits value of 0, returns just a single directory.
With a --splits value of 1, it also returns the results that --splits 0 on each
of the first result directory's children would give. And so on recursively for
larger numbers.

Think of the splits value as something like "depth" in to the tree, but not
quite, because it will give you the deepest directory past the split. The
default of 2 should give you useful results.

You can filter what files should be considered and reported on:

--groups: only consider files that have group ownership of one of these
          comma-separated groups.
--users:  only consider files that have user ownership of one of these
          comma-separated users.
--types:  only consider files that are one of these comma-separated file types,
          from this set of allowed values: cram,bam,index,compressed,
		  uncompressed,checkpoint,other,temp

Finally, to avoid producing too much output, the --minimum option can be used to
not display directories that have less than that number of MBs of data nested
inside. Defaults to 50MB.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			die("you must supply the path to the dgut.db")
		}

		if treeQueryDir == "" {
			die("you must supply a --dir you wish to query")
		}

		minSizeBytes := treeMinMBToBytes(treeMinimum)
		filter, err := makeTreeFilter(treeGroups, treeUsers, treeTypes)
		if err != nil {
			die(err.Error())
		}

		err = treeWhere(args[0], treeQueryDir, filter, treeSplits, minSizeBytes)
		if err != nil {
			die(err.Error())
		}
	},
}

// treeCreateCmd represents the tree create command.
var treeCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a db based on combine.dgut.gz files",
	Long: `Create a database based on the combine.dgut.gz files produced by
'wrstat combine' calls.

Within the given output directory, all the 'wrstat combine' combine.dgut.gz
files produced following multiple invocations of 'wrstat walk' followed by
'wrstat combine' will be read and stored in a single database file called
'dgut.db'.

NB: only call --create by adding it to wr with a dependency on the dependency
group all your 'wrstat combine' jobs are in.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			die("you must supply (only) the path to the dgut.db")
		}

		if err := createTreeDB(args[0]); err != nil {
			die("failed to create database: %s", err)
		}
	},
}

func init() {
	RootCmd.AddCommand(treeCmd)
	treeCmd.AddCommand((treeCreateCmd))

	// flags specific to these sub-commands
	treeCmd.Flags().StringVarP(&treeQueryDir, "dir", "d", "/",
		"directory path you wish to query")
	treeCmd.Flags().IntVarP(&treeSplits, "splits", "s", 2,
		"number of splits (see help text)")
	treeCmd.Flags().StringVarP(&treeGroups, "groups", "g", "",
		"comma separated list of unix groups to filter on")
	treeCmd.Flags().StringVarP(&treeUsers, "users", "u", "",
		"comma separated list of usernames to filter on")
	treeCmd.Flags().StringVarP(&treeTypes, "types", "t", "",
		"comma separated list of types (amongst cram,bam,index,compressed,uncompressed,checkpoint,other,temp) to filter on")
	treeCmd.Flags().IntVarP(&treeMinimum, "minimum", "m", 50,
		"minimum size (in MB) of files nested under a directory for it to be reported on")
}

// treeMinMBToBytes converts a number of MB to a number of bytes.
func treeMinMBToBytes(mbs int) uint64 {
	return uint64(mbs * bytesPerK * bytesPerK)
}

// makeTreeFilter creates a filter from our command line args.
func makeTreeFilter(groups, users, types string) (*dgut.Filter, error) {
	filter, err := makeTreeGroupFilter(groups)
	if err != nil {
		return nil, err
	}

	if err = addUsersToFilter(filter, users); err != nil {
		return nil, err
	}

	err = addTypesToFilter(filter, types)

	return filter, err
}

// makeTreeGroupFilter creates a filter for groups.
func makeTreeGroupFilter(groups string) (*dgut.Filter, error) {
	if groups == "" {
		return &dgut.Filter{}, nil
	}

	gnames := strings.Split(groups, ",")
	gids := make([]uint32, len(gnames))

	for i, name := range gnames {
		group, err := user.LookupGroup(name)
		if err != nil {
			return nil, err
		}

		gid, err := strconv.ParseUint(group.Gid, 10, 32)
		if err != nil {
			return nil, err
		}

		gids[i] = uint32(gid)
	}

	return &dgut.Filter{GIDs: gids}, nil
}

// addUsersToFilter adds a filter for users to the given filter.
func addUsersToFilter(filter *dgut.Filter, users string) error {
	if users == "" {
		return nil
	}

	unames := strings.Split(users, ",")
	uids := make([]uint32, len(unames))

	for i, name := range unames {
		user, err := user.Lookup(name)
		if err != nil {
			return err
		}

		uid, err := strconv.ParseUint(user.Uid, 10, 32)
		if err != nil {
			return err
		}

		uids[i] = uint32(uid)
	}

	filter.UIDs = uids

	return nil
}

// addTypesToFilter adds a filter for types to the given filter.
func addTypesToFilter(filter *dgut.Filter, types string) error {
	if types == "" {
		return nil
	}

	tnames := strings.Split(types, ",")
	fts := make([]summary.DirGUTFileType, len(tnames))

	for i, name := range tnames {
		ft, err := summary.FileTypeStringToDirGUTFileType(name)
		if err != nil {
			return err
		}

		fts[i] = ft
	}

	filter.FTs = fts

	return nil
}

// treeWhere does the main job of quering the dgut database to answer where the
// data is on disk.
func treeWhere(dbPath, queryDir string, filter *dgut.Filter, splits int, minSizeBytes uint64) error {
	dcss, err := findWhereDataIs(dbPath, queryDir, filter, splits)
	if err != nil {
		return err
	}

	printWhereDataIs(dcss, minSizeBytes)

	return nil
}

// findWhereDataIs access the database to get the query results.
func findWhereDataIs(dbPath, queryDir string, filter *dgut.Filter, splits int) (dgut.DCSs, error) {
	tree, err := dgut.NewTree(dbPath)
	if err != nil {
		return nil, err
	}

	return tree.Where(queryDir, filter, splits)
}

// printWhereDataIs formats query results and prints it to STDOUT.
func printWhereDataIs(dcss dgut.DCSs, minSizeBytes uint64) {
	if len(dcss) == 0 || (len(dcss) == 1 && dcss[0].Count == 0) {
		warn("no results")

		return
	}

	table := prepareWhereTable()

	for i, dcs := range dcss {
		if dcs.Size < minSizeBytes {
			defer printSkipped(len(dcss) - i)

			break
		}

		table.Append([]string{dcs.Dir, fmt.Sprintf("%d", dcs.Count), humanize.Bytes(dcs.Size)})
	}

	table.Render()
}

// prepareWhereTable creates a table with a header that outputs to STDOUT.
func prepareWhereTable() *tablewriter.Table {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Directory", "Count", "Size"})

	return table
}

// printSkipped prints the given number of results were skipped.
func printSkipped(n int) {
	warn(fmt.Sprintf("(%d results not displayed as smaller than --minimum)", n))
}

// createTreeDB does the main work of creating a database from combine.dgut.gz
// files.
func createTreeDB(dir string) error {
	sourceDir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}

	dguts, err := findDGUTOutputs(sourceDir)
	if err != nil {
		return err
	}

	outPath := filepath.Join(sourceDir, dgutDBBasename)
	db := dgut.NewDB(outPath)

	for _, path := range dguts {
		file, err := os.Open(path)
		if err != nil {
			return err
		}

		gz, err := gzip.NewReader(file)
		if err != nil {
			return err
		}

		if err = db.Store(gz, dgutStoreBatchSize); err != nil {
			return err
		}
	}

	return nil
}

// findDGUTOutputs finds the combine.dgut.gz files that should be in sub
// directories of the given directory. Errors if no files were found.
func findDGUTOutputs(sourceDir string) ([]string, error) {
	dguts, err := filepath.Glob(fmt.Sprintf("%s/*/*/%s", sourceDir, combineDGUTOutputFileBasename))
	if err != nil {
		return nil, err
	}

	if len(dguts) == 0 {
		return nil, errNoDGUTFilesFound
	}

	return dguts, nil
}
