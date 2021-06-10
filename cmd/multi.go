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
	"github.com/spf13/cobra"
)

// options for this cmd
var workDir string
var finalDir string

// multiCmd represents the multi command.
var multiCmd = &cobra.Command{
	Use:   "multi",
	Short: "Get stats on the contents of multiple directories",
	Long: `Get stats on the contents of multiple directories.

This calls 'wrstat dir' and 'wrstat combine' on each of the given directories of
interest. Their outputs go to unique subdirectories of the given
--working_directory.

Once everything has completed, the final output files are moved to the given
--final_output directory, and --working_directory is deleted.`,
	Run: func(cmd *cobra.Command, args []string) {
		if outputDir == "" {
			die("--output_directory is required")
		}
		if depGroup == "" {
			die("--dependecy_group is required")
		}
		if len(args) == 0 {
			die("at least 1 directory of interest must be supplied")
		}

		die("not yet implemented")
	},
}

func init() {
	RootCmd.AddCommand(multiCmd)

	// flags specific to this sub-command
	multiCmd.Flags().StringVarP(&workDir, "working_directory", "w", "", "base directory for intermediate results")
	multiCmd.Flags().StringVarP(&finalDir, "final_output", "f", "", "final output directory")
}
