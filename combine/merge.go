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
	"unicode/utf8"
)

var newline = []byte{'\n'} //nolint:gochecknoglobals

type fileLine struct {
	line  []byte
	index int
}

type readerHeap struct {
	readers           []bufio.Reader
	heap              []fileLine
	line              []byte
	unquoteComparison bool
}

func (rh *readerHeap) Len() int {
	return len(rh.heap)
}

func (rh *readerHeap) Push(fl fileLine) {
	cmp := compareLines

	if rh.unquoteComparison {
		cmp = compareQuotedPaths
	}

	pos, _ := slices.BinarySearchFunc(rh.heap, fl, cmp)

	rh.heap = slices.Insert(rh.heap, pos, fl)
}

func compareLines(a, b fileLine) int {
	return bytes.Compare(b.line, a.line)
}

type unquoter []byte

func (u *unquoter) Next() rune { //nolint:gocyclo,funlen,cyclop
	switch b := u.next(); b {
	case '\\':
		switch v := u.next(); v {
		case '\'':
			return '\''
		case '"':
			return '"'
		case 'a':
			return '\a'
		case 'b':
			return '\b'
		case 'f':
			return '\f'
		case 'n':
			return '\n'
		case 'r':
			return '\r'
		case 't':
			return '\t'
		case '0', '1', '2', '3', '4', '5', '6', '7':
			return u.readOctal(v)
		case 'x':
			return u.readHex(2) //nolint:mnd
		case 'u':
			return u.readHex(4) //nolint:mnd
		case 'U':
			return u.readHex(8) //nolint:mnd
		default:
			return -1
		}
	case '"':
		*u = (*u)[:0]

		return 0
	default:
		return b
	}
}

func (u *unquoter) next() rune {
	if len(*u) == 0 {
		return -1
	}

	r, l := utf8.DecodeRune(*u)

	*u = (*u)[l:]

	return r
}

func (u *unquoter) readOctal(v rune) rune {
	w := u.next()
	if w < '0' || w > '9' {
		return -1
	}

	x := u.next()
	if x < '0' || x > '9' {
		return -1
	}

	return (v - '0'<<6) | (w - '0'<<3) | (x - '0')
}

func (u *unquoter) readHex(n int) rune { //nolint:gocyclo
	var r rune

	for range n {
		r <<= 8

		x := u.next()
		if '0' <= x && x <= '9' { //nolint:gocritic,nestif
			r |= x - '0'
		} else if 'A' <= x && x <= 'F' {
			r |= x - 'A' + 10 //nolint:mnd
		} else if 'a' <= x && x <= 'f' {
			r |= x - 'a' + 10 //nolint:mnd
		} else {
			*u = (*u)[:0]

			return -1
		}
	}

	return r
}

func compareQuotedPaths(a, b fileLine) int {
	una := unquoter(a.line[1:])
	unb := unquoter(b.line[1:])

	for {
		a := una.Next()
		b := unb.Next()

		if a != b {
			return int(b - a)
		} else if a == -1 && b == -1 {
			return 0
		}
	}
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

	rh.Push(fileLine{
		line:  line,
		index: index,
	})

	return nil
}

// MergeSortedFiles merges pre-sorted files together.
func MergeSortedFiles(inputs []*os.File, unquoteComparison bool) (io.Reader, error) {
	rh := readerHeap{
		readers:           make([]bufio.Reader, len(inputs)),
		heap:              make([]fileLine, 0, len(inputs)),
		unquoteComparison: unquoteComparison,
	}

	for i, file := range inputs {
		rh.readers[i].Reset(file)

		if err := rh.pushToHeap(i); err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
	}

	return &rh, nil
}
