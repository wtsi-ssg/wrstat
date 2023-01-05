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
)

const numSummaryColumns = 2
const groupSumCols = 2

// GroupFiles combines pre-sorted .bygroup file data, by merging lines that
// share the same first 2 column values in to a single line.
func GroupFiles(inputs []*os.File, output *os.File) error {
	return Merge(inputs, output, mergeGroupStreamToFile)
}

// mergeGroupStreamToFile is a Merger that sums consecutive lines with the same
// first 2 columns.
func mergeGroupStreamToFile(data io.ReadCloser, output io.Writer) error {
	return MergeSummaryLines(data, groupSumCols, numSummaryColumns, sumCountAndSize, output)
}
