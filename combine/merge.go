/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
 *
 * Author: Rosie Kern <rk18@sanger.ac.uk>
 * 		   Michael Woolnough <mw31@sanger.ac.uk>
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
	"container/heap"
	"errors"
	"io"
	"os"
	"sync"
)

type fileLine struct {
	line  []byte
	index int
}

var fileLinePool = sync.Pool{ //nolint:gochecknoglobals
	New: func() any {
		return new(fileLine)
	},
}

type readerHeap struct {
	files   []*os.File
	readers []*bufio.Reader
	heap    []*fileLine
	line    []byte
}

func (rh readerHeap) Len() int { return len(rh.heap) }
func (rh readerHeap) Less(i, j int) bool {
	return bytes.Compare(rh.heap[i].line, rh.heap[j].line) < 0
}
func (rh readerHeap) Swap(i, j int) { rh.heap[i], rh.heap[j] = rh.heap[j], rh.heap[i] }

func (rh *readerHeap) Push(x any) {
	rh.heap = append(rh.heap, x.(*fileLine)) //nolint:forcetypeassert
}

func (rh *readerHeap) Pop() any {
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

	fileline := heap.Pop(rh).(*fileLine) //nolint:errcheck,forcetypeassert
	defer fileLinePool.Put(fileline)

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

	fileLine := fileLinePool.Get().(*fileLine) //nolint:errcheck,forcetypeassert
	fileLine.line = line
	fileLine.index = index

	heap.Push(rh, fileLine)

	return nil
}

func (rh *readerHeap) Close() error {
	for _, f := range rh.files {
		f.Close()
	}

	return nil
}

// MergeSortedFiles merges pre-sorted files together.
func MergeSortedFiles(inputs []string) (io.ReadCloser, error) {
	rh := readerHeap{
		files:   make([]*os.File, len(inputs)),
		readers: make([]*bufio.Reader, len(inputs)),
		heap:    make([]*fileLine, 0, len(inputs)),
	}

	for i, input := range inputs {
		file, err := os.Open(input)
		if err != nil {
			return nil, err
		}

		rh.files[i] = file
		rh.readers[i] = bufio.NewReader(file)
	}

	heap.Init(&rh)

	for i := range rh.readers {
		err := rh.pushToHeap(i)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
	}

	return &rh, nil
}
