/*******************************************************************************
 * Copyright (c) 2021-2023 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * Author: Kyle Mace <km34@sanger.ac.uk>
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
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/v6/neaten"
)

// destDirPerms are the permissions of the dest directory, to be used in making
// it if it does not already exist.
const destDirPerms = 0770

const disableDeletion = false

// options for this cmd.
var tidyDir string

// tidyCmd represents the tidy command.
var tidyCmd = &cobra.Command{
	Use:   "tidy",
	Short: "Tidy up multi output.",
	Long: `Tidy up multi output.

This is called by 'wrstat multi' after the main work has completed. It moves
final output files from the supplied unique working directory to the
--final_output directory, then deletes the working direcory.

multi creates a unique ("multi unique") directory, in which it creates
directories named after the basename of the directory of interest
("interest basename"), in which it creates another unique directory ("interest
unique"), in which it creates the output files.

tidy assumes the working directory you give it is the "multi unique" from multi.
It probably won't do the right thing if not.

Final output files are named to include the given --date as follows:
[date]_[interest basename].[interest unique].[multi unique].[suffix]

Where [suffix] is one of 'stats.gz' or 'logs.gz'.

Finally, if --date is set, it sets the modtime of the --final_output directory
to the requested time (in seconds since Unix epoch).

The output files will be given the same user:group ownership and
user,group,other read & write permissions as the --final_output directory.

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

		sourceDir, err := filepath.Abs(args[0])
		if err != nil {
			die("could not determine absolute path to source dir: %s", err)
		}

		tidy := neaten.Tidy{
			SrcDir:  sourceDir,
			DestDir: destDir,

			CombineFileSuffixes: map[string]string{
				combineStatsOutputFileBasename: "stats.gz",
				combineLogOutputFileBasename:   "logs.gz",
			},

			CombineFileGlobPattern:  "%s/%s",
			WalkFilePathGlobPattern: "%s/*%s",

			DestDirPerms: destDirPerms,
		}

		if err = tidy.Up(disableDeletion); err != nil {
			die("could not neaten dir: %s", err)
		}
	},
}

func init() {
	RootCmd.AddCommand(tidyCmd)

	// flags specific to this sub-command
	tidyCmd.Flags().StringVarP(&tidyDir, "final_output", "f", "", "final output directory")
}
