/*******************************************************************************
 * Copyright (c) 2021 Genome Research Ltd.
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
)

const bytesInMB = 1000000
const pgzipWriterBlocksMultiplier = 2
const combineStatsOutputFileBasename = "combine.stats.gz"
const combineUserGroupOutputFileBasename = "combine.byusergroup.gz"

// combineCmd represents the combine command.
var combineCmd = &cobra.Command{
	Use:   "combine",
	Short: "Combine the .stats files produced by 'wrstat walk'",
	Long: `Combine the .stats files produced by 'wrstat walk'.

Within the given output directory, all the 'wrstat stat' *.stats files produced
following an invocation of 'wrstat walk' will be concatenated, compressed and
placed at the root of the output directory in a file called 'combine.stats.gz'.

Additionally, all the 'wrstat stat' *.byusergroup files will be merged,
compressed and placed at the root of the output directory in a file called
'combine.byusergroup.gz'.

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
	cmd := exec.Command("sort", "-m", "--files0-from", "-")

	sortStdin, err := cmd.StdinPipe()
	if err != nil {
		return err
	}

	sortMergeOutput, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	if err = cmd.Start(); err != nil {
		return err
	}

	if err = sendFilePathsToSort(sortStdin, inputs); err != nil {
		return err
	}

	if err = mergeUserGroupStreamToCompressedFile(sortMergeOutput, output); err != nil {
		return err
	}

	if err = cmd.Wait(); err != nil {
		return err
	}

	return nil
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

	scanner := bufio.NewScanner(data)
	previous := []string{"", "", "", "", ""}

	for scanner.Scan() {
		current := strings.Split(scanner.Text(), "\t")

		if userGroupLinesMatch(previous, current) {
			mergeUserGroupLines(previous, current)

			continue
		}

		if previous[0] != "" {
			if _, err := zw.Write([]byte(strings.Join(previous, "\t") + "\n")); err != nil {
				return err
			}
		}

		previous = current
	}

	closeOutput()

	return nil
}

// userGroupLinesMatch returns true if the first 3 elements of a match the first
// 3 elements of b, corresponding to username, group and directory matching in
// 2 lines of byusergroup file output.
func userGroupLinesMatch(a, b []string) bool {
	return a[0] == b[0] && a[1] == b[1] && a[2] == b[2]
}

// mergeUserGroupLines will sum the 4th element of a and b and store the result
// in a[3], and likewise for the 5th element in a[4]. This corresponds to
// summing the file count and size columns of 2 lines in a byusergroup file.
func mergeUserGroupLines(a, b []string) {
	a[3] = addNumberStrings(a[3], b[3])
	a[4] = addNumberStrings(a[4], b[4])
}

// addNumberStrings treats a and b as ints, adds them together, and returns the
// resulting int64 as a string.
func addNumberStrings(a, b string) string {
	return strconv.FormatInt(atoi(a)+atoi(b), 10)
}

// atoi is like strconv.Atoi but returns an int64 and dies on error.
func atoi(n string) int64 {
	i, err := strconv.ParseInt(n, 10, 0)
	if err != nil {
		die("bad number string '%s': %s", n, err)
	}

	return i
}
