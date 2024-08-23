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
	"sync"

	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/v5/combine"
	"github.com/wtsi-ssg/wrstat/v5/fs"
)

const combineStatsOutputFileBasename = "combine.stats.gz"
const combineUserGroupOutputFileBasename = "combine.byusergroup.gz"
const combineGroupOutputFileBasename = "combine.bygroup"
const combineDGUTOutputFileBasename = "combine.dgut.db"
const combineLogOutputFileBasename = "combine.log.gz"

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

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			concatenateAndCompressStatsFiles(sourceDir)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			mergeAndCompressUserGroupFiles(sourceDir)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			mergeGroupFiles(sourceDir)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			mergeDGUTFilesToDB(sourceDir)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			concatenateAndCompressLogFiles(sourceDir)
		}()

		wg.Wait()
	},
}

func init() {
	RootCmd.AddCommand(combineCmd)
}

// concatenateAndCompressStatsFiles finds and concatenates the stats files and
// compresses the output.
func concatenateAndCompressStatsFiles(sourceDir string) {
	inputFiles, outputFile, err := fs.FindOpenAndCreate(sourceDir, sourceDir, statOutputFileSuffix,
		combineStatsOutputFileBasename)
	if err != nil {
		die("failed to find, open or create stats files: %s", err)
	}

	if err = combine.StatFiles(inputFiles, outputFile); err != nil {
		die("failed to concatenate and compress stats files (err: %s)", err)
	}
}

// mergeAndCompressUserGroupFiles finds and merges the byusergroup files and
// compresses the output.
func mergeAndCompressUserGroupFiles(sourceDir string) {
	inputFiles, outputFile, err := fs.FindOpenAndCreate(sourceDir,
		sourceDir, statUserGroupSummaryOutputFileSuffix, combineUserGroupOutputFileBasename)
	if err != nil {
		die("failed to find, open or create usergroup files: %s", err)
	}

	if err = combine.UserGroupFiles(inputFiles, outputFile); err != nil {
		die("failed to merge the user group files: %s", err)
	}
}

// mergeGroupFiles finds and merges the bygroup files.
func mergeGroupFiles(sourceDir string) {
	inputFiles, outputFile, err := fs.FindOpenAndCreate(sourceDir, sourceDir,
		statGroupSummaryOutputFileSuffix, combineGroupOutputFileBasename)
	if err != nil {
		die("failed to find, open or create group files: %s", err)
	}

	if err = combine.GroupFiles(inputFiles, outputFile); err != nil {
		die("failed to merge the group files: %s", err)
	}
}

// concatenateAndCompressLogFiles finds and merges the log files and compresses the
// output.
func concatenateAndCompressLogFiles(sourceDir string) {
	inputFiles, outputFile, err := fs.FindOpenAndCreate(sourceDir,
		sourceDir, statLogOutputFileSuffix, combineLogOutputFileBasename)
	if err != nil {
		die("failed to find, open or create log files: %s", err)
	}

	if err := combine.LogFiles(inputFiles, outputFile); err != nil {
		die("failed to merge the log files: %s", err)
	}
}

// mergeDGUTFilesToDB finds and merges the dgut files and then stores the
// information in a database.
func mergeDGUTFilesToDB(sourceDir string) {
	paths, err := fs.FindFilePathsInDir(sourceDir, statDGUTSummaryOutputFileSuffix)
	if err != nil {
		die("failed to find the dgut files: %s", err)
	}

	outputDir := filepath.Join(sourceDir, combineDGUTOutputFileBasename)

	if err = fs.RemoveAndCreateDir(outputDir); err != nil {
		die("failed to remove or create the dgut directory: %s", err)
	}

	if err = combine.DgutFiles(paths, outputDir); err != nil {
		die("failed to merge the dgut files: %s", err)
	}
}
