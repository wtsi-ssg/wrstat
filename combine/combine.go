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
	"runtime"

	"github.com/klauspost/pgzip"
)

const bytesInMB = 1000000
const pgzipWriterBlocksMultiplier = 2

// ConcatenateAndCompress takes a list of open files as its input, and an open
// file for its output. It writes to the output the compressed, concatenated
// inputs.
func ConcatenateAndCompress(inputs []*os.File, output *os.File, unquoteComparison bool) error {
	compressor := pgzip.NewWriter(output)

	err := compressor.SetConcurrency(bytesInMB, runtime.GOMAXPROCS(0)*pgzipWriterBlocksMultiplier)
	if err != nil {
		return err
	}

	r, err := MergeSortedFiles(inputs, unquoteComparison)
	if err != nil {
		return err
	}

	if _, err := io.Copy(compressor, r); err != nil {
		return err
	}

	return compressor.Close()
}
