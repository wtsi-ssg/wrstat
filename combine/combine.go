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

package combine

import (
	"bufio"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/klauspost/pgzip"
	"github.com/wtsi-ssg/wrstat/dgut"
	"github.com/wtsi-ssg/wrstat/fs"
)

const bytesInMB = 1000000
const pgzipWriterBlocksMultiplier = 2
const combineDGUTOutputFileBasename = "combine.dgut.db"
const numSummaryColumns = 2
const numSummaryColumnsDGUT = 3
const groupSumCols = 2
const userGroupSumCols = 3
const intBase = 10
const dgutSumCols = 4
const dgutStoreBatchSize = 100000

const statDGUTSummaryOutputFileSuffix = ".dgut"
const userOnlyPerm = 448

// MergeAndOptionallyCompressFiles identifies files from the supplied suffix,
// merges them, and then may or may not compress them based upon the type of
// function supplied as mergeAndStreamFunc.
func MergeAndOptionallyCompressFiles(sourceDir, inputFileSuffix, outputFileBasename string,
	mergeAndStreamFunc mergeStreamToOutputFunc) error {
	paths, err := fs.FindFilePathsInDir(sourceDir, inputFileSuffix)
	if err != nil {
		return err
	}

	output, err := fs.CreateOutputFileInDir(sourceDir, outputFileBasename)
	if err != nil {
		return err
	}

	err = mergeFilesAndStreamToOutput(paths, output, mergeAndStreamFunc)
	if err != nil {
		return err
	}

	return nil
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

func Concatenate(inputs []*os.File, output io.Writer) error {
	buf := make([]byte, bytesInMB)

	for _, input := range inputs {
		if _, err := io.CopyBuffer(output, input, buf); err != nil {
			return err
		}

		if err := input.Close(); err != nil {
			return err
		}
	}

	return nil
}

// CompressAndConcatenate compresses and concatenates the inputs and stores in
// the output.
func CompressAndConcatenate(sourceDir, inputFileSuffix, outputFile string) error {
	paths, err := fs.FindFilePathsInDir(sourceDir, inputFileSuffix)
	if err != nil {
		return err
	}

	inputs, err := fs.OpenFiles(paths)
	if err != nil {
		return err
	}

	output, err := fs.CreateOutputFileInDir(sourceDir, outputFile)
	if err != nil {
		return err
	}

	compressedOutput, closeOutput, err := Compress(output)
	if err != nil {
		return err
	}

	if err = Concatenate(inputs, compressedOutput); err != nil {
		return err
	}

	closeOutput()

	return nil
}

// Compress wraps the given output to compress data copied to it, and
// returns the writer. Also returns a function that you should call to close
// the writer and output when you're done.
func Compress(output *os.File) (*pgzip.Writer, func(), error) {
	compressedOutput := pgzip.NewWriter(output)

	err := compressedOutput.SetConcurrency(bytesInMB, runtime.GOMAXPROCS(0)*pgzipWriterBlocksMultiplier)
	if err != nil {
		return nil, nil, err
	}

	return compressedOutput, func() {
		err = compressedOutput.Close()
		if err != nil {
			return
		}

		err = output.Close()
		if err != nil {
			return
		}
	}, err
}

// mergeUserGroupStreamToCompressedFile merges pre-sorted (pre-merged) usergroup
// data (eg. from a `sort -m` of .byusergroup files), summing consecutive lines
// with the first 3 columns, and outputting the results to a file, compressed.
func mergeUserGroupStreamToCompressedFile(data io.ReadCloser, output *os.File) error {
	zw, closeOutput, err := Compress(output)
	if err != nil {
		return err
	}

	if err := MergeSummaryLines(data, userGroupSumCols, numSummaryColumns, sumCountAndSize, zw); err != nil {
		return err
	}

	closeOutput()

	return nil
}

// MergeSummaryLines merges pre-sorted (pre-merged) summary data (eg. from a
// `sort -m` of .by* files), summing consecutive lines that have the same values
// in the first matchColumns columns, and outputting the results.
func MergeSummaryLines(data io.ReadCloser, matchColumns, summaryColumns int,
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
	i, _ := strconv.ParseInt(n, intBase, 0)

	return i
}

// mergeGroupStreamToFile merges pre-sorted (pre-merged) group data
// (eg. from a `sort -m` of .bygroup files), summing consecutive lines with
// the same first 2 columns, and outputting the results.
func mergeGroupStreamToFile(data io.ReadCloser, output *os.File) error {
	if err := MergeSummaryLines(data, groupSumCols, numSummaryColumns, sumCountAndSize, output); err != nil {
		return err
	}

	return output.Close()
}

// MergeDGUTFilesToDB finds and merges the dgut files and then stores the
// information in a database.
func MergeDGUTFilesToDB(sourceDir string) error {
	paths, err := fs.FindFilePathsInDir(sourceDir, statDGUTSummaryOutputFileSuffix)
	if err != nil {
		return err
	}

	outputDir, err := createCombineDGUTOutputDir(sourceDir)
	if err != nil {
		return err
	}

	err = mergeDGUTAndStoreInDB(paths, outputDir)
	if err != nil {
		return err
	}

	return nil
}

// createCombineDGUTOutputDir creates a dgut output dir in the given dir.
// Returns the path to the created directory. If it already existed, will delete
// it first, since we can't store to a pre-existing db.
func createCombineDGUTOutputDir(dir string) (string, error) {
	path := filepath.Join(dir, combineDGUTOutputFileBasename)

	os.RemoveAll(path)

	err := os.MkdirAll(path, userOnlyPerm)
	if err != nil {
		return path, err
	}

	return path, nil
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

	if err = MergeSummaryLines(sortMergeOutput, dgutSumCols,
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

func Merge(inputData io.ReadCloser, dest io.Writer) error {
	if _, err := io.Copy(dest, inputData); err != nil {
		return err
	}

	return nil
}

// mergeLogStreamToCompressedFile combines log data, outputting the results to a
// file, compressed.
func mergeLogStreamToCompressedFile(data io.ReadCloser, output *os.File) error {
	compressedOutput, closeOutput, err := Compress(output)
	if err != nil {
		return err
	}

	if err := Merge(data, compressedOutput); err != nil {
		return err
	}

	closeOutput()

	return nil
}
