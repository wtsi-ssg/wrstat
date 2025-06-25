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
)

var (
	newline = []byte{'\n'} //nolint:gochecknoglobals

	ErrMissingOpeningSpeechMark = errors.New("missing opening speech mark")
	ErrMissingClosingSpeechMark = errors.New("missing closing speech mark")
	ErrInvalidQuote             = errors.New("invalid quote")
	ErrInvalidOctal             = errors.New("invalid octal escape")
	ErrInvalidHex               = errors.New("invalid hex escape")
)

type fileLine struct {
	line, unquotedLine []byte
	index              int
}

func compare(a, b fileLine) int {
	return bytes.Compare(b.unquotedLine, a.unquotedLine)
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
	pos, _ := slices.BinarySearchFunc(rh.heap, fl, compare)

	rh.heap = slices.Insert(rh.heap, pos, fl)
}

func unquote(str []byte) ([]byte, error) { //nolint:gocognit,gocyclo,cyclop,funlen
	if len(str) < 2 || str[0] != '"' {
		return nil, ErrMissingOpeningSpeechMark
	}

	var dst []byte

	str = str[1:]

	for len(str) != 0 {
		c := str[0]
		str = str[1:]

		switch c {
		case '\\':
			d := str[0]
			str = str[1:]

			var err error

			switch d {
			case '\'', '"', '\\':
				dst = append(dst, d)
			case 'a':
				dst = append(dst, '\a')
			case 'b':
				dst = append(dst, '\b')
			case 'f':
				dst = append(dst, '\f')
			case 'n':
				dst = append(dst, '\n')
			case 'r':
				dst = append(dst, '\r')
			case 't':
				dst = append(dst, '\t')
			case 'v':
				dst = append(dst, '\v')
			case '0', '1', '2', '3', '4', '5', '6', '7':
				dst, str, err = appendOctal(dst, str, d)
			case 'x':
				dst, str, err = appendHex(dst, str)
			case 'u':
				dst, str, err = appendHexes(dst, str, 2) //nolint:mnd
			case 'U':
				dst, str, err = appendHexes(dst, str, 4) //nolint:mnd
			default:
				err = ErrInvalidQuote
			}

			if err != nil {
				return nil, err
			}
		case '"':
			return dst, nil
		default:
			dst = append(dst, c)
		}
	}

	return nil, ErrMissingClosingSpeechMark
}

func appendOctal(dst, str []byte, c byte) ([]byte, []byte, error) {
	if len(str) < 2 || str[0] < '0' || str[0] >= '8' || str[1] < '0' || str[1] >= '8' {
		return nil, nil, ErrInvalidOctal
	}

	return append(dst, ((c-'0')<<6)|((str[0]-'0')<<3)|(str[1]-'0')), str[2:], nil //nolint:mnd
}

func appendHex(dst, str []byte) ([]byte, []byte, error) {
	if len(str) < 2 { //nolint:mnd
		return nil, nil, ErrInvalidHex
	}

	a, err := readHexNibble(str[0])
	if err != nil {
		return nil, nil, err
	}

	b, err := readHexNibble(str[1])
	if err != nil {
		return nil, nil, err
	}

	return append(dst, a<<4|b), str[2:], nil
}

func appendHexes(dst, str []byte, n int) ([]byte, []byte, error) {
	var err error

	for range n {
		if dst, str, err = appendHex(dst, str); err != nil {
			return nil, nil, err
		}
	}

	return dst, str, nil
}

func readHexNibble(x byte) (byte, error) {
	if '0' <= x && x <= '9' { //nolint:gocritic,nestif
		return x - '0', nil
	} else if 'A' <= x && x <= 'F' {
		return x - 'A' + 10, nil //nolint:mnd
	} else if 'a' <= x && x <= 'f' {
		return x - 'a' + 10, nil //nolint:mnd
	}

	return 0, ErrInvalidHex
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

	fl := fileLine{
		line:         line,
		unquotedLine: line,
		index:        index,
	}

	if rh.unquoteComparison {
		if fl.unquotedLine, err = unquote(line); err != nil {
			return err
		}
	}

	rh.Push(fl)

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
