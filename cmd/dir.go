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
	"github.com/wtsi-ssg/wrstat/hashdir"
)

// options for this cmd.
var outputDir string
var depGroup string

// dirCmd represents the dir command.
var dirCmd = &cobra.Command{
	Use:   "dir",
	Short: "Get stats on the contents of a directory",
	Long: `Get stats on the contents of a directory.

Within the given output directory, hashed folders are created to contain the
output file.

For each file in the directory of interest, stats about the file are written to
the output file.

For each sub directory within the directory of interest, a job is added to wr's
queue that calls this command again with all the same arguments, changing only
the directory of interest to this sub directory. The job will be in the given
dependency group.

NB: when this exits, that does not mean all stats have necessarily been
retrieved. You should wait until all jobs in the given dependency group have
completed (eg. by adding your own job that depends on that group, such as a
'wrstat combine' call).`,
	Run: func(cmd *cobra.Command, args []string) {
		if outputDir == "" {
			die("--output_directory is required")
		}
		if depGroup == "" {
			die("--dependecy_group is required")
		}
		if len(args) != 1 {
			die("exactly 1 directory of interest must be supplied")
		}

		desiredDir := args[0]

		h := hashdir.New(hashdir.RecommendedLevels)
		outFile, err := h.MkDirHashed(outputDir, desiredDir)
		if err != nil {
			die("failed to create output file: %s", err)
		}
		defer outFile.Close()

		die("not yet implemented")
	},
}

func init() {
	RootCmd.AddCommand(dirCmd)

	// flags specific to this sub-command
	dirCmd.Flags().StringVarP(&outputDir, "output_directory", "o", "", "base directory for output files")
	dirCmd.Flags().StringVarP(
		&depGroup,
		"dependency_group", "d", "",
		"dependency group that recursive jobs added to wr will belong to")
}
