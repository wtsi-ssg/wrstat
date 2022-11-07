/*******************************************************************************
 * Copyright (c) 2021-2022 Genome Research Ltd.
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

package combine

import (
	"fmt"
	"io"
	"os"

	"github.com/klauspost/pgzip"
)

const bytesInMB = 1000000

func ConcatenateAndCompress(inputs []*os.File, output *os.File) error {
	compressor, closer, err := Compress(output)
	if err != nil {
		return err
	}

	err = Concatenate(inputs, compressor)
	if err != nil {
		return err
	}

	closer()

	return nil
}

func Concatenate(inputs []*os.File, output io.Writer) error {
	buf := make([]byte, bytesInMB)

	for _, input := range inputs {
		if _, err := io.CopyBuffer(output, input, buf); err != nil {
			return fmt.Errorf("failed to concatenate: %w", err)
		}

		if err := input.Close(); err != nil {
			return fmt.Errorf("failed to close an input file: %w", err)
		}
	}

	return nil
}

func Compress(output *os.File) (*pgzip.Writer, func(), error) {
	zw := pgzip.NewWriter(output)

	return zw, func() {
		err := zw.Close()
		if err != nil {
			return
		}

		err = output.Close()
		if err != nil {
			return
		}
	}, nil
}
