/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
 *
 * Authors:
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
	"bufio"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/v5/internal/encode"
)

const (
	statsColumn       = 0
	byUserGroupColumn = 2
)

// chtsvCmd represents the chtsv command.
var decodeCmd = &cobra.Command{
	Use:   "decode",
	Short: "Decode custom base64 decoding in output files.",
	Long: `Decode custom base64 decoding in output files.

This command takes filenames as arguments and for each will determine, based on
the filename, whether it is a 'stats' or 'byusergroup' file and replace relevant
column in the file with the decoded form and print the file to stdout.

Files with names ending in '.gz' will be decompressed before printing.

Other files are simply copied to stdout.
`,
	Run: func(_ *cobra.Command, args []string) {
		for _, file := range args {
			if err := decodeFile(file); err != nil {
				die("%s", err)
			}
		}
	},
}

func init() {
	RootCmd.AddCommand(decodeCmd)
}

func decodeFile(file string) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}

	defer f.Close()

	var r io.Reader = f

	if strings.HasSuffix(file, ".gz") {
		gr, errr := gzip.NewReader(f)
		if errr != nil {
			return errr
		}

		file = file[:len(file)-3]
		r = gr
	}

	col := statsColumn

	if strings.HasSuffix(file, ".byusergroup") {
		col = byUserGroupColumn
	} else if !strings.HasSuffix(file, ".stats") {
		_, err = io.Copy(os.Stdout, r)

		return err
	}

	return decodeColumn(r, col)
}

func decodeColumn(r io.Reader, col int) error { //nolint:gocognit
	br := bufio.NewReader(r)

	w := bufio.NewWriter(os.Stdout)
	defer w.Flush()

	for {
		line, err := br.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return err
		}

		if line == "\n" {
			if _, err = w.WriteString(line); err != nil {
				return err
			}

			continue
		}

		if err = decodeColAndPrintLine(w, line, col); err != nil {
			return err
		}
	}
}

func decodeColAndPrintLine(w *bufio.Writer, line string, col int) error {
	var err error

	parts := strings.SplitN(line, "\t", col+2) //nolint:mnd

	if parts[col], err = encode.Base64Decode(parts[col]); err != nil {
		return err
	}

	if _, err := w.WriteString(parts[0]); err != nil {
		return err
	}

	for _, p := range parts[1:] {
		if err := w.WriteByte('\t'); err != nil {
			return err
		}

		if _, err := w.WriteString(p); err != nil {
			return err
		}
	}

	return nil
}
