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
	"sort"

	"github.com/dustin/go-humanize" //nolint:misspell
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/dgut"
	"github.com/wtsi-ssg/wrstat/server"
)

const bytesPerK = 1024
const defaultSplits = 2
const defaultMinMB = 50

// options for this cmd.
var whereQueryDir string
var whereSplits int
var whereGroups string
var whereUsers string
var whereTypes string
var whereMinimum int
var whereCert string
var whereJSON bool
var whereOrder string

// whereCmd represents the where command.
var whereCmd = &cobra.Command{
	Use:   "where",
	Short: "Find out where data is on disks",
	Long: `Find out where data is on disks.

Query the wrstat server by providing its URL in the form domain:port (using the
WRSTAT_SERVER environment variable, or overriding that with a command line
argument), and the --dir you wish to know about (defaults to the root
directory).

This tool will show where data really lies: the deepest directory that has all
filter-passing files nested under it.

With a --splits value of 0, returns just a single directory.
With a --splits value of 1, it also returns the results that --splits 0 on each
of the first result directory's children would give. And so on recursively for
larger numbers.

Think of the splits value as something like "depth" in to the filesystem tree,
but not quite, because it will give you the deepest directory past the split.
The default of 2 should give you useful results.

You can filter what files should be considered and reported on:

--groups: only consider files that have group ownership of one of these
          comma-separated groups.
--users:  only consider files that have user ownership of one of these
          comma-separated users.
--types:  only consider files that are one of these comma-separated file types,
          from this set of allowed values: cram,bam,index,compressed,
		  uncompressed,checkpoint,other,temp

To avoid producing too much output, the --minimum option can be used to not
display directories that have less than that number of MBs of data nested
inside. Defaults to 50MB.

You can change the sort --order from the default of by 'size', to by 'count' or
'dir'.

--minimum and --sort are ignored, however, if you choose --json output, which
will just give you all the filtered results. In the JSON output, the Size is in
bytes.

If the wrstat server is using an untrusted certificate, the path to its
certificate can be provided with --cert, or the WRSTAT_SERVER_CERT environment
variable, to force trust in it.
`,
	Run: func(cmd *cobra.Command, args []string) {
		setCLIFormat()

		var url string

		if len(args) == 1 {
			url = args[0]
		} else {
			url = os.Getenv("WRSTAT_SERVER")

			if url == "" {
				die("you must supply the server url")
			}
		}

		if whereCert == "" {
			whereCert = os.Getenv("WRSTAT_SERVER_CERT")
		}

		if whereQueryDir == "" {
			die("you must supply a --dir you wish to query")
		}

		minSizeBytes := whereMinMBToBytes(whereMinimum)

		err := where(url, whereCert, whereQueryDir, whereGroups, whereUsers, whereTypes,
			fmt.Sprintf("%d", whereSplits), whereOrder, minSizeBytes, whereJSON)
		if err != nil {
			die(err.Error())
		}
	},
}

func init() {
	RootCmd.AddCommand(whereCmd)

	// flags specific to these sub-commands
	whereCmd.Flags().StringVarP(&whereQueryDir, "dir", "d", "/",
		"directory path you wish to query")
	whereCmd.Flags().IntVarP(&whereSplits, "splits", "s", defaultSplits,
		"number of splits (see help text)")
	whereCmd.Flags().StringVarP(&whereGroups, "groups", "g", "",
		"comma separated list of unix groups to filter on")
	whereCmd.Flags().StringVarP(&whereUsers, "users", "u", "",
		"comma separated list of usernames to filter on")
	whereCmd.Flags().StringVarP(&whereTypes, "types", "t", "",
		"comma separated list of types (amongst cram,bam,index,compressed,uncompressed,checkpoint,other,temp) to filter on")
	whereCmd.Flags().IntVarP(&whereMinimum, "minimum", "m", defaultMinMB,
		"minimum size (in MB) of files nested under a directory for it to be reported on")
	whereCmd.Flags().StringVarP(&whereCert, "cert", "c", "",
		"path to the server's certificate to force trust in it")
	whereCmd.Flags().StringVarP(&whereOrder, "order", "o", "size",
		"sort order of results; size, count or dir")
	whereCmd.Flags().BoolVarP(&whereJSON, "json", "j", false,
		"output JSON (ignores --minimum and --order)")

	whereCmd.SetHelpFunc(func(command *cobra.Command, strings []string) {
		hideGlobalFlags(whereCmd, command, strings)
	})
}

// whereMinMBToBytes converts a number of MB to a number of bytes.
func whereMinMBToBytes(mbs int) uint64 {
	return uint64(mbs * bytesPerK * bytesPerK)
}

// where does the main job of quering the server to answer where the data is on
// disk.
func where(url, cert, dir, groups, users, types, splits, order string, minSizeBytes uint64, json bool) error {
	body, dcss, err := server.GetWhereDataIs(url, cert, dir, groups, users, types, splits)
	if err != nil {
		return err
	}

	if json {
		cliPrint(string(body))

		return nil
	}

	orderDCSS(dcss, order)

	printWhereDataIs(dcss, minSizeBytes)

	return nil
}

// orderDCSS reorders the given DCSs by count or dir, does nothing if order is
// size (the default) or invalid.
func orderDCSS(dcss dgut.DCSs, order string) {
	switch order {
	case "count":
		sort.Slice(dcss, func(i, j int) bool {
			return dcss[i].Count > dcss[j].Count
		})
	case "dir":
		sort.Slice(dcss, func(i, j int) bool {
			return dcss[i].Dir < dcss[j].Dir
		})
	}
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
