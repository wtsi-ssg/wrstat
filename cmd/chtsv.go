/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
 *   Michael Woolnough <mw31@sanger.ac.uk>
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

	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/v5/ch"
)

// chtsvCmd represents the chtsv command.
var chtsvCmd = &cobra.Command{
	Use:   "chtsv",
	Short: "Check format of ch.tsv.",
	Long: `Check format of ch.tsv.

This command is used to test the formatting of a ch.tsv file, and will inform of
a successful parsing, or report where the (first) error is located.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			die("need to supply location of ch.tsv file.")
		}

		f, err := os.Open(args[0])
		if err != nil {
			die("failed to open file: %s", err)
		}

		defer f.Close()

		tr := ch.NewTSVReader(f)

		for tr.Next() { //nolint:revive
		}

		if err = tr.Error(); err != nil {
			die("failed to parse file: %s", err)
		}

		info("TSV parsed successfully.")
	},
}

func init() {
	RootCmd.AddCommand(chtsvCmd)
}
