/*******************************************************************************
 * Copyright (c) 2021-2022 Genome Research Ltd.
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
	"path/filepath"

	"github.com/spf13/cobra"
<<<<<<< HEAD
	"github.com/wtsi-ssg/wrstat/v3/dgut"
=======
	"github.com/wtsi-ssg/wrstat/combine"
>>>>>>> Rearranged Functions and Increased Generalisation
)

const bytesInMB = 1000000
const pgzipWriterBlocksMultiplier = 2
const combineStatsOutputFileBasename = "combine.stats.gz"
const combineUserGroupOutputFileBasename = "combine.byusergroup.gz"
const combineGroupOutputFileBasename = "combine.bygroup"
const combineDGUTOutputFileBasename = "combine.dgut.db"
const combineLogOutputFileBasename = "combine.log.gz"
const numSummaryColumns = 2
const numSummaryColumnsDGUT = 3
const groupSumCols = 2
const userGroupSumCols = 3
const intBase = 10
const dgutSumCols = 4
const dgutStoreBatchSize = 100000

// combineCmd represents the combine command.
var combineCmd = &cobra.Command{
	Use:   "combine",
	Short: "Combine the .stats files produced by 'wrstat walk'",
	Long: `Combine the .stats files produced by 'wrstat walk'.

Within the given output directory, all the 'wrstat stat' *.stats files produced
following an invocation of 'wrstat walk' will be concatenated, compressed and
placed at the root of the output directory in a file called 'combine.stats.gz'.

Likewise, all the 'wrstat stat' *.byusergroup files will be merged,
compressed and placed at the root of the output directory in a file called
'combine.byusergroup.gz'.

The same applies to the *.log files, being called 'combine.log.gz'.

The *.dugt files will be turned in to databases in a directory
'combine.dgut.db'.

The *.bygroup files are merged but not compressed and called 'combine.bygroup'.

NB: only call this by adding it to wr with a dependency on the dependency group
you supplied 'wrstat walk'.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			die("exactly 1 'wrstat walk' output directory must be supplied")
		}

		sourceDir, err := filepath.Abs(args[0])
		if err != nil {
			die("could not get the absolute path to [%s]: %s", args[0], err)
		}

		// Note to self: this is the start of the API which is to
		// take a struct
		combine := combine.Combine{
			SourceDir: sourceDir,
		}

		err = combine.Combine()
		if err != nil {
			die("could not combine dir: %s", err)
		}
	},
}

func init() {
	RootCmd.AddCommand(combineCmd)
}
