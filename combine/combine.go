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
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strings"

	"github.com/klauspost/pgzip"
)

const bytesInMB = 1000000
const pgzipWriterBlocksMultiplier = 2

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

func Concatenate(inputs []*os.File, output io.Writer) error {
	buf := make([]byte, bytesInMB)

	for _, input := range inputs {
		if _, err := io.CopyBuffer(output, input, buf); err != nil {
			return fmt.Errorf("failed to concatenate: %w", err)
		}

		if err := input.Close(); err != nil {
			return fmt.Errorf("failed to close an input file: %w", err)
		}
	}

	return nil
}

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

// Merger is one of our merge*StreamTo* functions.
type Merger func(data io.ReadCloser, output io.Writer) error

// Merge merges the inputs files and streams the content to the streamFunc.
func Merge(inputs []*os.File, output io.Writer, streamFunc Merger) error {
	inputFiles := make([]string, len(inputs))
	for i, file := range inputs {
		inputFiles[i] = file.Name()
	}

	sortMergeOutput, cleanup, err := mergeSortedFiles(inputFiles)
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

// MatchingSummaryLineMerger is a func used by mergeSummaryLines() to handle
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
