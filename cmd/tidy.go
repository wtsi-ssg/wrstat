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
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	shutil "github.com/termie/go-shutil"
)

// options for this cmd.
var tidyDir string
var tidyDate string

// tidyCmd represents the tidy command.
var tidyCmd = &cobra.Command{
	Use:   "tidy",
	Short: "Tidy up multi output.",
	Long: `Tidy up multi output.

This is called by 'wrstat multi' after the main work has completed. It moves
final output files from the supplied unique working directory to the
--final_output directory, then deletes the working direcory.

multi creates a unique ("multi unique") directory , in which it creates
directories named after the basename of the directory of interest
("interest basename"), in which it creates another unique directory ("interest
unique"), in which it creates the output files.

tidy assumes the working directory you give it is the "multi unique" from multi.
It probably won't do the right thing if not.

Final output files are named to include the given --date as follows:
[date]_[interest basename].[interest unique].[multi unique].[type].gz

Where [type] is one of 'stats' or 'byusergroup'.

Once all output files have been moved, the "multi unique" directory is deleted.

It is safe to call this multiple times if it was, for example, killed half way
through; it won't clobber final outputs already moved.`,
	Run: func(cmd *cobra.Command, args []string) {
		if tidyDir == "" {
			die("--final_output is required")
		}
		if len(args) != 1 {
			die("exactly 1 unique working directory from 'wrstat multi' must be supplied")
		}

		destDir, err := filepath.Abs(tidyDir)
		if err != nil {
			die("could not determine absolute path to --final_output dir: %s", err)
		}

		err = os.MkdirAll(destDir, userOnlyPerm)
		if err != nil {
			die("failed to create --final_output dir [%s]: %s", destDir, err)
		}

		sourceDir, err := filepath.Abs(args[0])
		if err != nil {
			die("could not determine absolute path to source dir: %s", err)
		}

		err = moveAndDelete(sourceDir, destDir, tidyDate)
		if err != nil {
			die("failed to tidy: %s", err)
		}
	},
}

func init() {
	RootCmd.AddCommand(tidyCmd)

	// flags specific to this sub-command
	tidyCmd.Flags().StringVarP(&tidyDir, "final_output", "f", "", "final output directory")
	tidyCmd.Flags().StringVarP(&tidyDate, "date", "d", "", "datestamp of when 'wrstat multi' was called")
}

// moveAndDelete does the main work of this cmd.
func moveAndDelete(sourceDir, destDir, date string) error {
	if err := findAndMoveOutputs(sourceDir, destDir, date,
		combineStatsOutputFileBasename, "stats.gz"); err != nil {
		return err
	}

	if err := findAndMoveOutputs(sourceDir, destDir, date,
		combineUserGroupOutputFileBasename, "byusergroup.gz"); err != nil {
		return err
	}

	if err := findAndMoveOutputs(sourceDir, destDir, date,
		combineGroupOutputFileBasename, "bygroup"); err != nil {
		return err
	}

	return os.RemoveAll(sourceDir)
}

// findAndMoveOutputs finds output files in the given sourceDir with given
// suffix and moves them to destDir, including date in the name.
func findAndMoveOutputs(sourceDir, destDir, date, inputSuffix, outputSuffix string) error {
	outputPaths, err := filepath.Glob(fmt.Sprintf("%s/*/*/%s", sourceDir, inputSuffix))
	if err != nil {
		return err
	}

	err = moveOutputs(outputPaths, destDir, date, outputSuffix)
	if err != nil {
		return err
	}

	return nil
}

// moveOutputs moves each output file to the finalDir and changes its name to
// the correct format.
func moveOutputs(outputPaths []string, destDir, date, suffix string) error {
	for _, path := range outputPaths {
		err := moveOutput(path, destDir, date, suffix)
		if err != nil {
			return err
		}
	}

	return nil
}

// moveOutput moves an output file to the finalDir and changes its name to
// the correct format.
func moveOutput(source string, destDir, date, suffix string) error {
	interestUniqueDir := filepath.Dir(source)
	interestBaseDir := filepath.Dir(interestUniqueDir)
	multiUniqueDir := filepath.Dir(interestBaseDir)
	dest := filepath.Join(destDir, fmt.Sprintf("%s_%s.%s.%s.%s",
		date,
		filepath.Base(interestBaseDir),
		filepath.Base(interestUniqueDir),
		filepath.Base(multiUniqueDir),
		suffix))

	err := os.Rename(source, dest)
	if err != nil {
		err = shutil.CopyFile(source, dest, false)
	}

	return err
}
