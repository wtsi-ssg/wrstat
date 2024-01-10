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
)

const mergeArgs = 2

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
 
 If the second one doesn't have databases with the same date prefix and its
 .dgut.dbs.updated file is older than the firsts, this command waits up to 23hrs
 for it to exist before giving up. If the second's .dgut.dbs.updated file is
 newer than the firsts, waits up to 23hrs for the first's most recent db to have
 the same prefex as the most recent one in the second, and merges that one.
 
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
	},
}

func init() {
	RootCmd.AddCommand(mergedbsCmd)

	// flags specific to this sub-command
	mergedbsCmd.Flags().BoolVarP(&mergeDelete, "delete", "d", false,
		"delete all output from first directory after merge")
}
