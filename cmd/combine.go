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
	"bufio"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/klauspost/pgzip"
	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/dgut"
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
	paths := findStatFilePaths(sourceDir)
	inputs := openFiles(paths)
	output := createCombineStatsOutputFile(sourceDir)

	concatenateAndCompress(inputs, output)
}

// findStatFilePaths returns files in the given dir named with a '.stats'
// suffix.
func findStatFilePaths(dir string) []string {
	return findFilePathsInDir(dir, statOutputFileSuffix)
}

// findFilePathsInDir finds files in the given dir that have basenames with the
// given suffix. Dies on error.
func findFilePathsInDir(dir, suffix string) []string {
	paths, err := filepath.Glob(fmt.Sprintf("%s/*%s", dir, suffix))
	if err != nil || len(paths) == 0 {
		die("failed to find input files based on [%s/*%s] (err: %s)", dir, suffix, err)
	}

	return paths
}

// openFiles opens the given files for reading.
func openFiles(paths []string) []*os.File {
	files := make([]*os.File, len(paths))

	for i, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			die("failed to open a .stats files: %s", err)
		}

		files[i] = file
	}

	return files
}

// createCombineStatsOutputFile creates a stats output file in the given dir.
func createCombineStatsOutputFile(dir string) *os.File {
	return createOutputFileInDir(dir, combineStatsOutputFileBasename)
}

// createOutputFileInDir creates a file for writing in the given dir with the
// given basename. Dies on error.
func createOutputFileInDir(dir, basename string) *os.File {
	file, err := os.Create(filepath.Join(dir, basename))
	if err != nil {
		die("failed to create output file: %s", err)
	}

	return file
}

// concatenateAndCompress concatenates and compresses the inputs and stores in
// the output.
func concatenateAndCompress(inputs []*os.File, output *os.File) {
	zw, closeOutput := compressOutput(output)

	buf := make([]byte, bytesInMB)

	for _, input := range inputs {
		if _, err := io.CopyBuffer(zw, input, buf); err != nil {
			die("failed to concatenate and compress: %s", err)
		}

		if err := input.Close(); err != nil {
			warn("failed to close an input file: %s", err)
		}
	}

	closeOutput()
}

// compressOutput wraps the given output to compress data copied to it, and
// returns the writer. Also returns a function that you should call to close
// the writer and output when you're done.
func compressOutput(output *os.File) (*pgzip.Writer, func()) {
	zw := pgzip.NewWriter(output)

	err := zw.SetConcurrency(bytesInMB, runtime.GOMAXPROCS(0)*pgzipWriterBlocksMultiplier)
	if err != nil {
		die("failed to set up compression: %s", err)
	}

	return zw, func() {
		err = zw.Close()
		if err != nil {
			die("failed to close output file: %s", err)
		}

		err = output.Close()
		if err != nil {
			die("failed to close output file: %s", err)
		}
	}
}

// mergeAndCompressUserGroupFiles finds and merges the byusergroup files and
// compresses the output.
func mergeAndCompressUserGroupFiles(sourceDir string) {
	paths := findUserGroupFilePaths(sourceDir)
	output := createCombineUserGroupOutputFile(sourceDir)

	err := mergeUserGroupAndCompress(paths, output)
	if err != nil {
		die("failed to merge the byusergroup files: %s", err)
	}
}

// findUserGroupFilePaths returns files in the given dir named with a
// '.byusergroup' suffix.
func findUserGroupFilePaths(dir string) []string {
	return findFilePathsInDir(dir, statUserGroupSummaryOutputFileSuffix)
}

// createCombineUserGroupOutputFile creates a usergroup output file in the given
// dir.
func createCombineUserGroupOutputFile(dir string) *os.File {
	return createOutputFileInDir(dir, combineUserGroupOutputFileBasename)
}

// mergeUserGroupAndCompress merges the inputs and stores in the output,
// compressed.
func mergeUserGroupAndCompress(inputs []string, output *os.File) error {
	return mergeFilesAndStreamToOutput(inputs, output, mergeUserGroupStreamToCompressedFile)
}

// mergeStreamToOutputFunc is one of our merge*StreamTo* functions.
type mergeStreamToOutputFunc func(data io.ReadCloser, output *os.File) error

// mergeFilesAndStreamToOutput merges the inputs files and streams the content
// to the streamFunc.
func mergeFilesAndStreamToOutput(inputs []string, output *os.File, streamFunc mergeStreamToOutputFunc) error {
	sortMergeOutput, cleanup, err := mergeSortedFiles(inputs)
	if err != nil {
		return err
	}

	if err = streamFunc(sortMergeOutput, output); err != nil {
		return err
	}

	if err = cleanup(); err != nil {
		return err
	}

	return nil
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
func mergeUserGroupStreamToCompressedFile(data io.ReadCloser, output *os.File) error {
	zw, closeOutput := compressOutput(output)

	if err := mergeSummaryLines(data, userGroupSumCols, numSummaryColumns, sumCountAndSize, zw); err != nil {
		return err
	}

	closeOutput()

	return nil
}

// mergeSummaryLines merges pre-sorted (pre-merged) summary data (eg. from a
// `sort -m` of .by* files), summing consecutive lines that have the same values
// in the first matchColumns columns, and outputting the results.
func mergeSummaryLines(data io.ReadCloser, matchColumns, summaryColumns int,
	mslm matchingSummaryLineMerger, output io.Writer) error {
	scanner := bufio.NewScanner(data)
	previous := make([]string, matchColumns+summaryColumns)

	for scanner.Scan() {
		current := strings.Split(scanner.Text(), "\t")

		if summaryLinesMatch(matchColumns, previous, current) {
			mslm(summaryColumns, previous, current)

			continue
		}

		if previous[0] != "" {
			if _, err := output.Write([]byte(strings.Join(previous, "\t") + "\n")); err != nil {
				return err
			}
		}

		previous = current
	}

	_, err := output.Write([]byte(strings.Join(previous, "\t") + "\n"))

	return err
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
	paths := findGroupFilePaths(sourceDir)
	output := createCombineGroupOutputFile(sourceDir)

	err := mergeGroups(paths, output)
	if err != nil {
		die("failed to merge the bygroup files: %s", err)
	}
}

// findGroupFilePaths returns files in the given dir named with a
// '.bygroup' suffix.
func findGroupFilePaths(dir string) []string {
	return findFilePathsInDir(dir, statGroupSummaryOutputFileSuffix)
}

// createCombineGroupOutputFile creates a usergroup output file in the given
// dir.
func createCombineGroupOutputFile(dir string) *os.File {
	return createOutputFileInDir(dir, combineGroupOutputFileBasename)
}

// mergeGroups merges and outputs bygroup data.
func mergeGroups(inputs []string, output *os.File) error {
	return mergeFilesAndStreamToOutput(inputs, output, mergeGroupStreamToFile)
}

// mergeGroupStreamToFile merges pre-sorted (pre-merged) group data
// (eg. from a `sort -m` of .bygroup files), summing consecutive lines with
// the same first 2 columns, and outputting the results.
func mergeGroupStreamToFile(data io.ReadCloser, output *os.File) error {
	if err := mergeSummaryLines(data, groupSumCols, numSummaryColumns, sumCountAndSize, output); err != nil {
		return err
	}

	return output.Close()
}

// mergeDGUTFilesToDB finds and merges the dgut files and then stores the
// information in a database.
func mergeDGUTFilesToDB(sourceDir string) {
	paths := findDGUTFilePaths(sourceDir)
	outputDir := createCombineDGUTOutputDir(sourceDir)

	err := mergeDGUTAndStoreInDB(paths, outputDir)
	if err != nil {
		die("failed to merge the dgut files: %s", err)
	}
}

// findDGUTFilePaths returns files in the given dir named with a '.dgut' suffix.
func findDGUTFilePaths(dir string) []string {
	return findFilePathsInDir(dir, statDGUTSummaryOutputFileSuffix)
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

	if err = mergeSummaryLines(sortMergeOutput, dgutSumCols,
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
	paths := findLogFilePaths(sourceDir)
	output := createCombineLogOutputFile(sourceDir)

	err := mergeLogAndCompress(paths, output)
	if err != nil {
		die("failed to merge the log files: %s", err)
	}
}

// findLogFilePaths returns files in the given dir named with a '.log' suffix.
func findLogFilePaths(dir string) []string {
	return findFilePathsInDir(dir, statLogOutputFileSuffix)
}

// createCombineLogOutputFile creates a log output file in the given dir.
func createCombineLogOutputFile(dir string) *os.File {
	return createOutputFileInDir(dir, combineLogOutputFileBasename)
}

// mergeLogAndCompress merges the inputs and stores in the output, compressed.
func mergeLogAndCompress(inputs []string, output *os.File) error {
	return mergeFilesAndStreamToOutput(inputs, output, mergeLogStreamToCompressedFile)
}

// mergeLogStreamToCompressedFile combines log data, outputting the results to a
// file, compressed.
func mergeLogStreamToCompressedFile(data io.ReadCloser, output *os.File) error {
	zw, closeOutput := compressOutput(output)

	if _, err := io.Copy(zw, data); err != nil {
		return err
	}

	closeOutput()

	return nil
}
