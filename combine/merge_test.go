/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
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

package combine

import (
	"io"
	"os"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMergeSortedFiles(t *testing.T) {
	Convey("", t, func() {
		for _, test := range [...]struct {
			Inputs            []string
			Output            string
			UnquoteComparison bool
		}{
			{
				Inputs: []string{"abc\ndef", "abd\ncba"},
				Output: "abc\nabd\ncba\ndef\n",
			},
			{
				Inputs: []string{"\"abc\"\n\"def\"", "\"abd\"\n\"cba\""},
				Output: "\"abc\"\n\"abd\"\n\"cba\"\n\"def\"\n",
			},
			{
				Inputs:            []string{"\"abc\"\n\"def\"", "\"abd\"\n\"cba\""},
				Output:            "\"abc\"\n\"abd\"\n\"cba\"\n\"def\"\n",
				UnquoteComparison: true,
			},
			{
				Inputs:            []string{"\"ab\\nc\"\n\"def\"", "\"ab d\"\n\"cba\""},
				Output:            "\"ab\\nc\"\n\"ab d\"\n\"cba\"\n\"def\"\n",
				UnquoteComparison: true,
			},
			{
				Inputs: []string{"\"ab\\nc\"\n\"def\"", "\"ab d\"\n\"cba\""},
				Output: "\"ab d\"\n\"ab\\nc\"\n\"cba\"\n\"def\"\n",
			},
			{
				Inputs: []string{"a\nb\nc", "d\ne\nf\ng", "h"},
				Output: "a\nb\nc\nd\ne\nf\ng\nh\n",
			},
			{
				Inputs: []string{
					"\"/a/b/c/d\"\t3\t2\t1\t3\n\"/a/b/c/\"\t0\t10\t2\t3",
					"\"/a/b/c/dz\"\t3\t2\t1\t3\n\"/a/b/cz/\"\t0\t10\t2\t3",
				},
				Output: "\"/a/b/c/d\"\t3\t2\t1\t3\n\"/a/b/c/\"\t0\t10\t2\t3\n" +
					"\"/a/b/c/dz\"\t3\t2\t1\t3\n\"/a/b/cz/\"\t0\t10\t2\t3\n",
				UnquoteComparison: true,
			},
		} {
			files := make([]*os.File, len(test.Inputs))

			for n, input := range test.Inputs {
				r, w, err := os.Pipe()
				So(err, ShouldBeNil)

				files[n] = r

				go func() {
					w.WriteString(input) //nolint:errcheck
					w.Close()
				}()
			}

			var output strings.Builder

			r, err := MergeSortedFiles(files, test.UnquoteComparison)
			So(err, ShouldBeNil)

			_, err = io.Copy(&output, r)
			So(err, ShouldBeNil)
			So(output.String(), ShouldEqual, test.Output)
		}
	})
}
