/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
 *          Sendu Bala <sb10@sanger.ac.uk>
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

package ch

import (
	"bufio"
	"errors"
	"io"
	"strings"
)

// directory user group fileperms dirperms
//
// *perms format is rwxrwxrwx for user,group,other, where - means remove the
// permission, * means leave it unchanged, and a letter means set it. s for the
// group x would enable setting group sticky bit. s implies x. Could use ^ in at
// least 2 equivalent places to mean "set all if any set". ie. '**^**^***` would
// mean "change nothing, except if execute is set on user or group, set it on
// both".
//
// user and group can be unix username or unix group name. * means don't set it.
// Could use ^ to mean copy from the directory.

const numCols = 5

var errInvalidFormat = errors.New("invalid ch.tsv format")

type chTSVReader struct {
	r       *bufio.Reader
	Columns [numCols]string
	err     error
}

func NewCHTSVReader(r io.Reader) *chTSVReader {
	return &chTSVReader{
		r: bufio.NewReader(r),
	}
}

func (t *chTSVReader) Next() bool {
	line, err := t.r.ReadString('\n')
	if err != nil {
		if !errors.Is(err, io.EOF) {
			t.err = err
		} else if line != "" {
			t.err = io.ErrUnexpectedEOF
		}

		return false
	}

	line = strings.TrimSuffix(line, "\n")

	if line == "" {
		return false
	}

	for i := numCols - 1; i > 0; i-- {
		line, t.Columns[i] = splitLastTab(line)
	}

	t.Columns[0] = line

	if !validPerms(t.Columns[3]) || !validPerms(t.Columns[4]) {
		t.err = errInvalidFormat

		return false
	}

	return true
}

func splitLastTab(str string) (string, string) {
	pos := strings.LastIndexByte(str, '\t')

	if pos == -1 {
		return "", str
	}

	return str[:pos], str[pos+1:]
}

func validPerms(perms string) bool {
	if len(perms) != 9 {
		return false
	}

	return true
}

func (t *chTSVReader) Error() error {
	return t.err
}
