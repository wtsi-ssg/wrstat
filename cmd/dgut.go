/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/dgut"
)

type Error string

func (e Error) Error() string { return string(e) }

const errNoDGUTFilesFound = Error("no combine.dgut.gz files were found")

const dgutDBBasename = "dgut.db"

const dgutStoreBatchSize = 10000

// dgutCmd represents the dgut command.
var dgutCmd = &cobra.Command{
	Use:   "dgut",
	Short: "Create a db based on combine.dgut.gz files",
	Long: `Create a database based on the combine.dgut.gz files produced by
'wrstat combine' calls.

Within the given output directory, all the 'wrstat combine' combine.dgut.gz
files produced following multiple invocations of 'wrstat walk' followed by
'wrstat combine' will be read and stored in database directories called
'dgut.db.*', where * is an incrementing number.

NB: only call this by adding it to wr with a dependency on the dependency
group all your 'wrstat combine' jobs are in.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			die("you must supply (only) the path to the dgut.db")
		}

		if err := createDGUTDBs(args[0]); err != nil {
			die("failed to create database: %s", err)
		}
	},
}

func init() {
	RootCmd.AddCommand(dgutCmd)
}

// createDGUTDBs does the main work of creating databases from combine.dgut.gz
// files.
func createDGUTDBs(dir string) error {
	sourceDir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}

	dguts, err := findDGUTOutputs(sourceDir)
	if err != nil {
		return err
	}

	for i, path := range dguts {
		file, err := os.Open(path)
		if err != nil {
			return err
		}

		gz, err := gzip.NewReader(file)
		if err != nil {
			return err
		}

		outPath := filepath.Join(sourceDir, fmt.Sprintf("%s.%d", dgutDBBasename, i))
		db := dgut.NewDB(outPath)

		if err = db.Store(gz, dgutStoreBatchSize); err != nil {
			return err
		}
	}

	return nil
}

// findDGUTOutputs finds the combine.dgut.gz files that should be in sub
// directories of the given directory. Errors if no files were found.
func findDGUTOutputs(sourceDir string) ([]string, error) {
	dguts, err := filepath.Glob(fmt.Sprintf("%s/*/*/%s", sourceDir, combineDGUTOutputFileBasename))
	if err != nil {
		return nil, err
	}

	if len(dguts) == 0 {
		return nil, errNoDGUTFilesFound
	}

	return dguts, nil
}
