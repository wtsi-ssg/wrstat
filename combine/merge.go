/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
 *
 * Author: Rosie Kern <rk18@sanger.ac.uk>
 *         Michael Woolnough <mw31@sanger.ac.uk>
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
	"bufio"
	"bytes"
	"errors"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"unsafe"
)

var newline = []byte{'\n'} //nolint:gochecknoglobals

type fileLine struct {
	comparator string
	line       []byte
	index      int
}

type readerHeap struct {
	readers []bufio.Reader
	heap    []fileLine
	line    []byte
}

func (rh *readerHeap) Len() int {
	return len(rh.heap)
}

func (rh *readerHeap) Push(fl fileLine) {
	pos, _ := slices.BinarySearchFunc(rh.heap, fl, func(a, b fileLine) int {
		return strings.Compare(b.comparator, a.comparator)
	})

	rh.heap = slices.Insert(rh.heap, pos, fl)
}

func (rh *readerHeap) Pop() fileLine {
	x := rh.heap[len(rh.heap)-1]
	rh.heap = rh.heap[:len(rh.heap)-1]

	return x
}

func (rh *readerHeap) Read(p []byte) (int, error) {
	line, err := rh.getLineFromHeap()
	if err != nil {
		return 0, err
	}

	n := copy(p, line)
	rh.line = line[n:]

	return n, nil
}

func (rh *readerHeap) getLineFromHeap() ([]byte, error) {
	if len(rh.line) != 0 {
		return rh.line, nil
	}

	if rh.Len() == 0 {
		return nil, io.EOF
	}

	fileline := rh.Pop()

	if err := rh.pushToHeap(fileline.index); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}

	return fileline.line, nil
}

func (rh *readerHeap) pushToHeap(index int) error {
	line, err := rh.readers[index].ReadBytes('\n')
	if err != nil {
		if !errors.Is(err, io.EOF) || len(line) == 0 {
			return err
		}
	}

	if !bytes.HasSuffix(line, newline) {
		line = append(line, '\n')
	}

	comparator := comparatorFromLine(line)

	rh.Push(fileLine{
		comparator: comparator,
		line:       line,
		index:      index,
	})

	return nil
}

func comparatorFromLine(line []byte) string {
	lineStr := unsafe.String(&line[0], len(line))
	comparator := lineStr

	if line[0] != '"' {
		return comparator
	}

	pos := bytes.IndexByte(line, '\t')
	if pos <= 0 {
		return comparator
	}

	comparator, err := strconv.Unquote(comparator)
	if err != nil {
		return lineStr
	}

	return comparator
}

// MergeSortedFiles merges pre-sorted files together.
func MergeSortedFiles(inputs []*os.File) (io.Reader, error) {
	rh := readerHeap{
		readers: make([]bufio.Reader, len(inputs)),
		heap:    make([]fileLine, 0, len(inputs)),
	}

	for i, file := range inputs {
		rh.readers[i].Reset(file)

		if err := rh.pushToHeap(i); err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
	}

	return &rh, nil
}
