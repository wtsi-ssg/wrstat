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
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/spf13/cobra"
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
	"github.com/wtsi-ssg/wrstat/v3/dgut"
=======
	"github.com/wtsi-ssg/wrstat/combine"
>>>>>>> Rearranged Functions and Increased Generalisation
=======
	"github.com/wtsi-ssg/wrstat/dgut"
>>>>>>> Rewritten Tests Using Public Methods
=======
	"github.com/wtsi-ssg/wrstat/combine"
	"github.com/wtsi-ssg/wrstat/dgut"
	"github.com/wtsi-ssg/wrstat/fs"
>>>>>>> Merge and Compression Testing
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
			mergeAndCompressLogFiles(sourceDir)
		}()

		wg.Wait()
	},
}

func init() {
	RootCmd.AddCommand(combineCmd)
}

// concatenateAndCompressStatsFiles finds and conatenates the stats files and
// compresses the output.
func concatenateAndCompressStatsFiles(sourceDir string) {
	paths, err := fs.FindFilePathsInDir(sourceDir, statOutputFileSuffix)
	if err != nil {
		die("failed to find input files (err: %s)", err)
	}

	inputs, err := fs.OpenFiles(paths)
	if err != nil {
		die("failed to open input files (err: %s)", err)

	}

	output, err := fs.CreateOutputFileInDir(sourceDir, combineStatsOutputFileBasename)
	if err != nil {
		die("failed to create output file (err: %s)", err)
	}

	if err = combine.ConcatenateAndCompress(inputs, output); err != nil {
		die("failed to concatenate and compress stats files (err: %s)", err)
	}
}

// mergeAndCompressUserGroupFiles finds and merges the byusergroup files and
// compresses the output.
func mergeAndCompressUserGroupFiles(sourceDir string) {
	paths, err := fs.FindFilePathsInDir(sourceDir, statUserGroupSummaryOutputFileSuffix)
	if err != nil {
		die("failed to find byusergroup files: %s", err)
	}

	output, err := fs.CreateOutputFileInDir(sourceDir, combineUserGroupOutputFileBasename)
	if err != nil {
		die("failed to create user group output file: %s", err)
	}

	err = mergeUserGroupAndCompress(paths, output)
	if err != nil {
		die("failed to merge the byusergroup files: %s", err)
	}
}

// mergeUserGroupAndCompress merges the inputs and stores in the output,
// compressed.
func mergeUserGroupAndCompress(inputs []string, output *os.File) error {
	inputFiles, err := fs.OpenFiles(inputs)
	if err != nil {
		return err
	}

	return combine.MergeAndCompress(inputFiles, output, mergeUserGroupStreamToCompressedFile)
}

// mergeSortedFiles shells out to `sort -m` to merge pre-sorted files together.
// Returns a pipe of the output from sort, and function you should call after
// you've finished reading the output to cleanup.
func mergeSortedFiles(inputs []string) (io.ReadCloser, func() error, error) {
	cmd := exec.Command("sort", "-m", "--files0-from", "-")
	cmd.Env = os.Environ()
	cmd.Env = append(cmd.Env, "LC_ALL=C")

	sortStdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, nil, err
	}

	sortMergeOutput, err := cmd.StdoutPipe()
	if err != nil {
		return nil, nil, err
	}

	if err = cmd.Start(); err != nil {
		return nil, nil, err
	}

	if err = sendFilePathsToSort(sortStdin, inputs); err != nil {
		return nil, nil, err
	}

	return sortMergeOutput, cmd.Wait, nil
}

// sendFilePathsToSort will pipe the given paths null terminated to the pipe.
// For use with the StdinPipe of an exec.Command for `sort -m --files0-from -`.
// The in is closed afterwards.
func sendFilePathsToSort(in io.WriteCloser, paths []string) error {
	for _, path := range paths {
		if _, err := in.Write([]byte(path + string(rune(0)))); err != nil {
			return err
		}
	}

	return in.Close()
}

// mergeUserGroupStreamToCompressedFile merges pre-sorted (pre-merged) usergroup
// data (eg. from a `sort -m` of .byusergroup files), summing consecutive lines
// with the first 3 columns, and outputting the results to a file, compressed.
func mergeUserGroupStreamToCompressedFile(data io.ReadCloser, output io.Writer) error {
	zw, closeOutput, err := combine.Compress(output)
	if err != nil {
		return err
	}

	if err := combine.MergeSummaryLines(data, userGroupSumCols, numSummaryColumns, sumCountAndSize, zw); err != nil {
		return err
	}

	closeOutput()

	return nil
}

// summaryLinesMatch returns true if the first matchColumns elements of 'a'
// match the first matchColums elements of 'b'.
func summaryLinesMatch(matchColumns int, a, b []string) bool {
	for i := 0; i < matchColumns; i++ {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

// matchingSummaryLineMerger is a func used by mergeSummaryLines() to handle
// summary columns when match columns match. a is the previous columns, b is the
// current. a should have its summary columns altered to merge information from
// b. Cols is the number of summary columns (the columns that contain info to
// eg. sum).
type matchingSummaryLineMerger func(cols int, a, b []string)

// sumCountAndSize is a matchingSummaryLineMerger that, given cols 2,  will sum
// the second to last element of a and b and store the result in a[penultimate],
// and likewise for the last element in a[last]. This corresponds to summing the
// file count and size columns of 2 lines in a by* file.
func sumCountAndSize(cols int, a, b []string) {
	last := len(a) - (cols - 1)
	penultimate := last - 1

	a[penultimate] = addNumberStrings(a[penultimate], b[penultimate])
	a[last] = addNumberStrings(a[last], b[last])
}

// sumCountAndSizeAndKeepOldestAtime needs cols 3, and is like sumCountAndSize,
// but keeps the oldest atime (smallest number) found in the last column.
func sumCountAndSizeAndKeepOldestAtime(cols int, a, b []string) {
	sumCountAndSize(cols, a, b)

	last := len(a) - 1

	if atoi(b[last]) < atoi(a[last]) {
		a[last] = b[last]
	}
}

// addNumberStrings treats a and b as ints, adds them together, and returns the
// resulting int64 as a string.
func addNumberStrings(a, b string) string {
	return strconv.FormatInt(atoi(a)+atoi(b), intBase)
}

// atoi is like strconv.Atoi but returns an int64 and dies on error.
func atoi(n string) int64 {
	i, err := strconv.ParseInt(n, intBase, 0)
	if err != nil {
		die("bad number string '%s': %s", n, err)
	}

	return i
}

// mergeGroupFiles finds and merges the bygroup files.
func mergeGroupFiles(sourceDir string) {
	paths, err := fs.FindFilePathsInDir(sourceDir, statGroupSummaryOutputFileSuffix)
	if err != nil {
		die("failed to find the the group files: %s", err)
	}

	output, err := fs.CreateOutputFileInDir(sourceDir, combineGroupOutputFileBasename)
	if err != nil {
		die("failed to find the the group files: %s", err)
	}

	if err = mergeGroups(paths, output); err != nil {
		die("failed to merge the group files: %s", err)
	}
}

// mergeGroups merges and outputs bygroup data.
func mergeGroups(inputs []string, output *os.File) error {
	inputFiles, err := fs.OpenFiles(inputs)
	if err != nil {
		return err
	}

	return combine.Merge(inputFiles, output, mergeGroupStreamToFile)
}

// mergeGroupStreamToFile merges pre-sorted (pre-merged) group data
// (eg. from a `sort -m` of .bygroup files), summing consecutive lines with
// the same first 2 columns, and outputting the results.
func mergeGroupStreamToFile(data io.ReadCloser, output io.Writer) error {
	if err := combine.MergeSummaryLines(data, groupSumCols, numSummaryColumns, sumCountAndSize, output); err != nil {
		return err
	}

	return nil
}

// mergeDGUTFilesToDB finds and merges the dgut files and then stores the
// information in a database.
func mergeDGUTFilesToDB(sourceDir string) {
	paths, err := fs.FindFilePathsInDir(sourceDir, statDGUTSummaryOutputFileSuffix)
	if err != nil {
		die("failed to find the dgut files: %s", err)
	}

	outputDir := createCombineDGUTOutputDir(sourceDir)

	if err = mergeDGUTAndStoreInDB(paths, outputDir); err != nil {
		die("failed to merge the dgut files: %s", err)
	}
}

// createCombineDGUTOutputDir creates a dgut output dir in the given dir.
// Returns the path to the created directory. If it already existed, will delete
// it first, since we can't store to a pre-existing db.
func createCombineDGUTOutputDir(dir string) string {
	path := filepath.Join(dir, combineDGUTOutputFileBasename)

	os.RemoveAll(path)

	err := os.MkdirAll(path, userOnlyPerm)
	if err != nil {
		die("failed to create output dir: %s", err)
	}

	return path
}

// mergeDGUTAndStoreInDB merges pre-sorted dgut data, summing consecutive lines
// with the same first 4 columns, and outputs the results to an embedded
// database.
func mergeDGUTAndStoreInDB(inputs []string, outputDir string) error {
	sortMergeOutput, cleanup, err := mergeSortedFiles(inputs)
	if err != nil {
		return err
	}

	db := dgut.NewDB(outputDir)
	reader, writer := io.Pipe()
	errCh := make(chan error, 1)

	go func() {
		errCh <- db.Store(reader, dgutStoreBatchSize)
	}()

	if err = combine.MergeSummaryLines(sortMergeOutput, dgutSumCols,
		numSummaryColumnsDGUT, sumCountAndSizeAndKeepOldestAtime, writer); err != nil {
		return err
	}

	if err = writer.Close(); err != nil {
		return err
	}

	err = <-errCh
	if err != nil {
		return err
	}

	return cleanup()
}

// mergeAndCompressLogFiles finds and merges the log files and compresses the
// output.
func mergeAndCompressLogFiles(sourceDir string) {
	paths, err := fs.FindFilePathsInDir(sourceDir, statLogOutputFileSuffix)
	if err != nil {
		die("failed to find the log files: %s", err)
	}

	output, err := fs.CreateOutputFileInDir(sourceDir, combineLogOutputFileBasename)
	if err != nil {
		die("failed to create the log output file: %s", err)
	}

	if err := mergeLogAndCompress(paths, output); err != nil {
		die("failed to merge the log files: %s", err)
	}
}

// mergeLogAndCompress merges the inputs and stores in the output, compressed.
func mergeLogAndCompress(inputs []string, output *os.File) error {
	inputFiles, err := fs.OpenFiles(inputs)
	if err != nil {
		return err
	}

	return combine.MergeAndCompress(inputFiles, output, mergeLogStreamToCompressedFile)
}

// mergeLogStreamToCompressedFile combines log data, outputting the results to a
// file, compressed.
func mergeLogStreamToCompressedFile(data io.ReadCloser, output io.Writer) error {
	zw, closeOutput, err := combine.Compress(output)
	if err != nil {
		return err
	}

	if _, err := io.Copy(zw, data); err != nil {
		return err
	}

	closeOutput()

	return nil
}
