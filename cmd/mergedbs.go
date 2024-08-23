/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
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
	"github.com/wtsi-ssg/wrstat/v5/merge"
)

const mergeArgs = 2

// options for this cmd.
var mergeDelete bool

// mergedbsCmd represents the mergedbs command.
var mergedbsCmd = &cobra.Command{
	Use:   "mergedbs",
	Short: "Merge wrstat databases.",
	Long: `Merge wrstat databases.

Used to merge in results from a minimal 'wrstat multi -p' run into the working
directory of a full 'wrstat multi' run.

Provide the output working directory of the minimal '-p' run and the unique
working directory of the full multi run.

Provide the --delete option to delete all previous runs within the minimal
working directory, leaving the latest.`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != mergeArgs {
			die("exactly 2 output directories from 'wrstat multi' must be supplied")
		}

		if err := merge.Merge(args[0], args[1], mergeDelete); err != nil {
			die("error while merging: %s", err)
		}
	},
}

func init() {
	RootCmd.AddCommand(mergedbsCmd)

	// flags specific to this sub-command
	mergedbsCmd.Flags().BoolVarP(&mergeDelete, "delete", "d", false,
		"delete all output from first directory after merge")
}
