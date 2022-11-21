/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * 		   Kyle Mace  <km34@sanger.ac.uk>
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
	"strconv"
)

const userGroupSumCols = 3
const intBase = 10

// UserGroupFiles merges the usergroup files into the output, compressed.
func UserGroupFiles(inputs []*os.File, output *os.File) error {
	return MergeAndCompress(inputs, output, mergeUserGroupStreamToCompressedFile)
}

// mergeUserGroupStreamToCompressedFile merges pre-sorted (pre-merged) usergroup
// data (eg. from a `sort -m` of .byusergroup files), summing consecutive lines
// with the first 3 columns, and outputting the results to a file, compressed.
func mergeUserGroupStreamToCompressedFile(data io.ReadCloser, output io.Writer) error {
	return MergeSummaryLines(data, userGroupSumCols, numSummaryColumns, sumCountAndSize, output)
}

// sumCountAndSize is a matchingSummaryLineMerger that, given cols 2,  will sum
// the second to last element of a and b and store the result in a[penultimate],
// and likewise for the last element in a[last]. This corresponds to summing the
// file count and size columns of 2 lines in a by* file.
func sumCountAndSize(cols int, a, b []string) {
	last := len(a) - (cols - 1)
	penultimate := last - 1

	a[penultimate] = addNumberStrings(a[penultimate], b[penultimate])
	a[last] = addNumberStrings(a[last], b[last])
}

// addNumberStrings treats a and b as ints, adds them together, and returns the
// resulting int64 as a string.
func addNumberStrings(a, b string) string {
	return strconv.FormatInt(atoi(a)+atoi(b), intBase)
}

// atoi is like strconv.Atoi but returns an int64 and dies on error.
func atoi(n string) int64 {
	i, err := strconv.ParseInt(n, intBase, 0)
	if err != nil {
		panic("Was not able to convert string.")
	}

	return i
}
