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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"

	"github.com/klauspost/pgzip"
	"github.com/spf13/cobra"
)

const bytesInMB = 1000000
const pgzipWriterBlocksMultiplier = 2
const combineOutputFileBasename = "combine.gz"

// combineCmd represents the combine command.
var combineCmd = &cobra.Command{
	Use:   "combine",
	Short: "Combine the .stats files produced by 'wrstat walk'",
	Long: `Combine the .stats files produced by 'wrstat walk'.

Within the given output directory, all the 'wrstat stat' files produced
following an invocation of 'wrstat walk' will be concatenated, compressed and
placed at the root of the output directory in a file called 'combine.gz'.

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

		paths := findStatFilePaths(sourceDir)
		inputs := openFiles(paths)
		output := createCombineOutputFile(sourceDir)

		concatenateAndCompress(inputs, output)
	},
}

func init() {
	RootCmd.AddCommand(combineCmd)
}

// findStatFilePaths returns files in the given dir named with a '.stats'
// suffix.
func findStatFilePaths(dir string) []string {
	paths, err := filepath.Glob(fmt.Sprintf("%s/*%s", dir, statOutputFileSuffix))
	if err != nil || len(paths) == 0 {
		die("failed to find .stats files: %s", err)
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

// createCombineOutputFile creates an output file in the given dir.
func createCombineOutputFile(dir string) *os.File {
	file, err := os.Create(filepath.Join(dir, combineOutputFileBasename))
	if err != nil {
		die("failed to create output file: %s", err)
	}

	return file
}

// concatenateAndCompress concatenates and compresses the inputs and stores in
// the output.
func concatenateAndCompress(inputs []*os.File, output *os.File) {
	zw := pgzip.NewWriter(output)

	err := zw.SetConcurrency(bytesInMB, runtime.GOMAXPROCS(0)*pgzipWriterBlocksMultiplier)
	if err != nil {
		die("failed to set up compression: %s", err)
	}

	buf := make([]byte, bytesInMB)

	for _, input := range inputs {
		_, err = io.CopyBuffer(zw, input, buf)
		if err != nil {
			die("failed to concatenate and compress: %s", err)
		}

		err = input.Close()
		if err != nil {
			warn("failed to close an input file: %s", err)
		}
	}

	err = zw.Close()
	if err != nil {
		die("failed to close output file: %s", err)
	}

	err = output.Close()
	if err != nil {
		die("failed to close output file: %s", err)
	}
}
