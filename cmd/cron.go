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
	"context"

	"github.com/adhocore/gronx"
	"github.com/adhocore/gronx/pkg/tasker"
	"github.com/spf13/cobra"
)

// options for this cmd.
var crontab string

// cronCmd represents the cron command.
var cronCmd = &cobra.Command{
	Use:   "cron",
	Short: "Run multi on a regular basis.",
	Long: `Run multi on a regular basis.

This command takes the same arguments as 'wrstat multi' and will run multi with
those arguments on the given --crontab schedule.

The default schedule is 8am every day.

This command will just run in the foreground forever until killed. You should
probably use the daemonize program to daemonize this instead.
`,
	Run: func(cmd *cobra.Command, args []string) {
		checkMultiArgs(args)

		if crontab == "" {
			die("--crontab must be supplied")
		}

		gron := gronx.New()

		if !gron.IsValid(crontab) {
			die("--crontab is invalid")
		}

		taskr := tasker.New(tasker.Option{})
		taskr.Task(crontab, func(ctx context.Context) (int, error) {
			err := doMultiScheduling(args)

			return 0, err
		})

		taskr.Run()
	},
}

func init() {
	RootCmd.AddCommand(cronCmd)

	// flags specific to this sub-command
	cronCmd.Flags().StringVarP(&workDir, "working_directory", "w", "", "base directory for intermediate results")
	cronCmd.Flags().StringVarP(&finalDir, "final_output", "f", "", "final output directory")
	cronCmd.Flags().IntVarP(&multiInodes, "inodes_per_stat", "n",
		defaultInodesPerJob, "number of inodes per parallel stat job")
	cronCmd.Flags().StringVar(&multiCh, "ch", "", "passed through to 'wrstat walk'")
	cronCmd.Flags().StringVarP(&forcedQueue, "queue", "q", "", "force a particular queue to be used when scheduling jobs")
	cronCmd.Flags().StringVarP(&crontab, "crontab", "c",
		"0 8 * * *",
		"crontab describing when to run, first 5 columns only")
}
