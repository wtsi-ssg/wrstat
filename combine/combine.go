/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * 		   Kyle Mace  <km34@sanger.ac.uk>
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
	"runtime"
	"strings"

	"github.com/klauspost/pgzip"
)

const bytesInMB = 1000000
const pgzipWriterBlocksMultiplier = 2

// ConcatenateAndCompress takes a list of open files as its input, and an open
// file for its output. It writes to the output the compressed, concatenated
// inputs.
func ConcatenateAndCompress(inputs []*os.File, output *os.File) error {
	compressor, closer, err := Compress(output)
	if err != nil {
		return err
	}

	err = Concatenate(inputs, compressor)
	if err != nil {
		return err
	}

	closer()

	return nil
}

// Concatenate takes a list of open files as its input, and an io.Writer as its
// output. It concatenates the contents of the inputs into the output.
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

// Compress takes an io writer as its input, and compresses it. It returns this,
// along with a function to close the writer and an error status.
func Compress(output io.Writer) (*pgzip.Writer, func(), error) {
	zw := pgzip.NewWriter(output)

	err := zw.SetConcurrency(bytesInMB, runtime.GOMAXPROCS(0)*pgzipWriterBlocksMultiplier)

	return zw, func() {
		err = zw.Close()
		if err != nil {
			return
		}

		if err != nil {
			return
		}
	}, err
}

// Merger takes an input io.readCloser and an output io.writer, and defines how
// we want to merge the content in the io.readCloser, and stream it to the
// output io.writer.
type Merger func(data io.ReadCloser, output io.Writer) error

// Merge merges the inputs files and streams the content to the streamFunc.
func Merge(inputs []*os.File, output io.Writer, streamFunc Merger) error {
	inputFiles := make([]string, len(inputs))
	for i, file := range inputs {
		inputFiles[i] = file.Name()
	}

	sortMergeOutput, cleanup, err := MergeSortedFiles(inputFiles)
	if err != nil {
		return err
	}

	if err = streamFunc(sortMergeOutput, output); err != nil {
		return err
	}

	return cleanup()
}

// MergeAndCompress takes a list of open files, an open output file, and a
// Merger function to express the details of how the file contents should be
// merged. It compresses the output, and stores the merged input contents in
// there.
func MergeAndCompress(inputs []*os.File, output *os.File, streamFunc Merger) error {
	zw, closer, err := Compress(output)
	if err != nil {
		return err
	}

	err = Merge(inputs, zw, streamFunc)
	if err != nil {
		return err
	}

	closer()

	return nil
}

// MergeSortedFiles shells out to `sort -m` to merge pre-sorted files together.
// Returns a pipe of the output from sort, and function you should call after
// you've finished reading the output to cleanup.
func MergeSortedFiles(inputs []string) (io.ReadCloser, func() error, error) {
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

// MergeSummaryLines merges pre-sorted (pre-merged) summary data (eg. from a
// `sort -m` of .by* files), summing consecutive lines that have the same values
// in the first matchColumns columns, and outputting the results.
func MergeSummaryLines(data io.ReadCloser, matchColumns, summaryColumns int,
	mslm MatchingSummaryLineMerger, output io.Writer) error {
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

// MatchingSummaryLineMerger is a func used by MergeSummaryLines() to handle
// summary columns when match columns match. a is the previous columns, b is the
// current. a should have its summary columns altered to merge information from
// b. Cols is the number of summary columns (the columns that contain info to
// eg. sum).
type MatchingSummaryLineMerger func(cols int, a, b []string)

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

// sumCountAndSizesAndKeepTimes sums summable columns using sumCountAndSizes(),
// and keeps the oldest atime (smallest number) and newest mtime (largest
// number).
func sumCountAndSizesAndKeepTimes(_ int, a, b []string) {
	sumCountAndSizes(a, b)

	if atoi(b[dgutAtimeColIndex]) < atoi(a[dgutAtimeColIndex]) {
		a[dgutAtimeColIndex] = b[dgutAtimeColIndex]
	}

	if atoi(b[dgutMtimeColIndex]) > atoi(a[dgutMtimeColIndex]) {
		a[dgutMtimeColIndex] = b[dgutMtimeColIndex]
	}
}

// sumCountAndSizes is a matchingSummaryLineMerger that, given cols 20, will sum
// the corresponding elements of a and b and store the result in a, except for
// the atime and mtime columns.
func sumCountAndSizes(a, b []string) {
	for i := dgutSumCols; i < len(a); i++ {
		if i == dgutAtimeColIndex || i == dgutMtimeColIndex {
			continue
		}

		a[i] = addNumberStrings(a[i], b[i])
	}
}
