/*******************************************************************************
 * Copyright (c) 2021 - 2022 Genome Research Ltd.
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
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/neaten"
)

// modeRW are the read-write permission bits for user, group and other.
const modeRW = 0666

// destDirPerms are the permissions of the dest directory, to be used in making
// it if it does not already exist.
const destDirPerms = 0700

const dgutDBsSuffix = "dgut.dbs"
const dgutDBsSentinelBasename = ".dgut.dbs.updated"

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
 
multi creates a unique ("multi unique") directory, in which it creates
directories named after the basename of the directory of interest
("interest basename"), in which it creates another unique directory ("interest
unique"), in which it creates the output files.
 
tidy assumes the working directory you give it is the "multi unique" from multi.
It probably won't do the right thing if not.
 
Final output files are named to include the given --date as follows:
[date]_[interest basename].[interest unique].[multi unique].[suffix]
 
Where [suffix] is one of 'stats.gz', 'byusergroup.gz', 'bygroup' or 'logs.gz'.
 
The base.dirs file directly inside the given "multi unique" directory is named:
[date]_[multi unique].basedirs
 
It also moves the combine.dgut.db directories to inside a directory named:
[date]_[multi unique].dgut.dbs
(making them sequentially numbered sub-directories)
 
Finally, it creates or touches a file named '.dgut.dbs.updated' in the
--final_output directory, giving it an mtime matching the oldest mtime of the
walk log files. 'wrstat server' will use this file to reload its database and
update its knowledge of when the data was captured.
 
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

		err = os.MkdirAll(destDir, userOnlyPerm)
		if err != nil {
			die("failed to create --final_output dir [%s]: %s", destDir, err)
		}

		sourceDir, err := filepath.Abs(args[0])
		if err != nil {
			die("could not determine absolute path to source dir: %s", err)
		}

		tidy := neaten.Tidy{
			SrcDir:  sourceDir,
			DestDir: destDir,
			Date:    tidyDate,

			CombineFileSuffixes: map[string]string{
				combineStatsOutputFileBasename:     "stats.gz",
				combineUserGroupOutputFileBasename: "byusergroup.gz",
				combineGroupOutputFileBasename:     "bygroup",
				combineLogOutputFileBasename:       "logs.gz"},

			DBFileSuffixes: map[string]string{
				combineDGUTOutputFileBasename: "dgut.dbs"},

			BaseFileSuffixes: map[string]string{
				basedirBasename: "basedirs"},

			CombineFileGlobPattern:  "%s/*/*/%s",
			DBFileGlobPattern:       "%s/*/*/%s",
			WalkFilePathGlobPattern: "%s/*/*/*%s",

			DestDirPerms: destDirPerms,
		}

		err = tidy.Up()
		if err != nil {
			die("could not neaten dir: %s", err)
		}
	},
}

func init() {
	RootCmd.AddCommand(tidyCmd)

	// flags specific to this sub-command
	tidyCmd.Flags().StringVarP(&tidyDir, "final_output", "f", "", "final output directory")
	tidyCmd.Flags().StringVarP(&tidyDate, "date", "d", "", "datestamp of when 'wrstat multi' was called")
}
