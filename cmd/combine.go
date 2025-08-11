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
	"os"
	"path/filepath"
	"sync"

	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/v6/combine"
	"github.com/wtsi-ssg/wrstat/v6/fs"
)

const combineStatsOutputFileBasename = "combine.stats.gz"
const combineLogOutputFileBasename = "combine.log.gz"

// combineCmd represents the combine command.
var combineCmd = &cobra.Command{
	Use:   "combine",
	Short: "Combine the files produced by 'wrstat walk'",
	Long: `Combine the files produced by 'wrstat walk'.
	
Within the given output directory, all the 'wrstat stat' *.stats files produced
following an invocation of 'wrstat walk' will be concatenated, compressed and
placed at the root of the output directory in a file called 'combine.stats.gz'.

The same applies to the *.log files, being called 'combine.log.gz'.

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

		go keepAliveCheck(sourceDir, "source directory no longer exists")

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()

			concatenateAndCompressStatsFiles(sourceDir)
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
	if !initCmds {
		return
	}

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

	closeFiles(inputFiles, outputFile)
}

func closeFiles(inputFiles []*os.File, outputFile *os.File) {
	for _, file := range inputFiles {
		file.Close()
	}

	if err := outputFile.Close(); err != nil {
		die("failed to close compressed stats file (err: %s)", err)
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

	closeFiles(inputFiles, outputFile)
}
