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
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/v4/basedirs"
	"github.com/wtsi-ssg/wrstat/v4/neaten"
	"github.com/wtsi-ssg/wrstat/v4/wait"
)

const (
	mergeArgs             = 2
	mergeDatePrefixLength = 8
	mergeMaxWait          = 23 * time.Hour
	reloadGrace           = 15 * time.Minute
)

// options for this cmd.
var mergeDelete bool

// mergedbsCmd represents the mergedbs command.
var mergedbsCmd = &cobra.Command{
	Use:   "mergedbs",
	Short: "Merge wrstat databases.",
	Long: `Merge wrstat databases.
 
 If you run 'wrstat multi' on 2 separate systems but want to combine their
 outputs to display on a single wrstat server, use this command to merge their
 databases.
 
 Provide the multi output directories of the 2 systems. The most recent database
 information in the first will be copied/merged in to the most recent set with
 the same date prefix in the second one, and then the second one's
 .dgut.dbs.updated will be touched to trigger any running server monitoring the
 second one's dbs to reload.
 
 This will wait up to 23hrs for both folder's most recent database files have
 the same date prefix.
 
 To avoid doing the merge in the middle of a server doing a database reload,
 waits until it is more than 15mins since the second's .dgut.dbs.updated was
 touched
 
 This means you can run multi on your 2 systems once per day, and run this in
 a crontab job once per day as well, and it will merge the 2 outputs of the
 same day once they're both ready.

 Provide the --delete option to delete all files with the database's date
 prefix from the first output directory after successful merge.
 .`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != mergeArgs {
			die("exactly 2 output directories from 'wrstat multi' must be supplied")
		}

		sourceDir, destDir := args[0], args[1]

		sourceDGUTDir, destDGUTDir, err := wait.ForMatchingPrefixOfLatestSuffix(
			dgutDBsSuffix, mergeDatePrefixLength, sourceDir, destDir, mergeMaxWait)
		if err != nil {
			die("wait for matching dgut.db outputs failed: %s", err)
		}

		err = neaten.MergeDGUTDBDirectories(sourceDGUTDir, destDGUTDir)
		if err != nil {
			die("merge of dgut.db directories failed: %s", err)
		}

		sourceBasedir, destBasedir, err := wait.ForMatchingPrefixOfLatestSuffix(
			basedirBasename, mergeDatePrefixLength, sourceDir, destDir, mergeMaxWait)
		if err != nil {
			die("wait for matching basedirs outputs failed: %s", err)
		}

		outputDBPath := destBasedir + ".merging"

		err = basedirs.MergeDBs(sourceBasedir, destBasedir, outputDBPath)
		if err != nil {
			die("merge of basedir.dbs failed: %s", err)
		}

		sentinal := filepath.Join(destDir, dgutDBsSentinelBasename)

		err = wait.UntilFileIsOld(sentinal, reloadGrace)
		if err != nil {
			die("waiting for the dgutdbs sentintal file failed: %s", err)
		}

		err = os.Rename(outputDBPath, destBasedir)
		if err != nil {
			die("failed to move the merged basedirs.db file back over original: %s", err)
		}

		err = neaten.Touch(sentinal)
		if err != nil {
			die("failed to touch the dgutdbs sentinal file: %s", err)
		}

		if mergeDelete {
			err = neaten.DeleteAllPrefixedDirEntries(sourceDir, filepath.Base(sourceBasedir)[:mergeDatePrefixLength])
			if err != nil {
				warn("failed to delete source files: %s", err)
			}
		}
	},
}

func init() {
	RootCmd.AddCommand(mergedbsCmd)

	// flags specific to this sub-command
	mergedbsCmd.Flags().BoolVarP(&mergeDelete, "delete", "d", false,
		"delete all output from first directory after merge")
}
