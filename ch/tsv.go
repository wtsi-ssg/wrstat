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
	"regexp"
	"strings"
)

const (
	numCols  = 5
	uoPermRE = "[r\\*\\^-][w\\*\\^-][x\\*\\^-]"
	gPermRE  = "[r\\*\\^-][w\\*\\^-][xs\\*\\^-]"
)

var (
	errInvalidFormat = errors.New("invalid ch.tsv format")
	permsRE          = regexp.MustCompile("^" + uoPermRE + gPermRE + uoPermRE + "$")
	nameRE           = regexp.MustCompile(`^[a-z_]([a-z0-9_-]{0,31}|[a-z0-9_-]{0,30}\\$)$`)
)

// TSVReader is for parsing the custom ch.tsv file format, which has tab
// separated columns:
//
// directory user group fileperms dirperms
//
// *perms format is rwxrwxrwx for user,group,other, where - means remove the
// permission, * means leave it unchanged, and one of [rwx] means set it. s for
// the group x would enable setting group sticky bit. s implies x. Using ^ in at
// least 2 equivalent places to mean "set all if any set". ie. '**^**^***` would
// mean "change nothing, except if execute is set on user or group, set it on
// both".
//
// user and group can be unix username or unix group name. * means don't set it.
// Use ^ to mean copy from the directory.
type TSVReader struct {
	r       *bufio.Reader
	columns [numCols]string
	err     error
}

// NewTSVReader returns a new TSVReader which can read ch.tsv files.
func NewTSVReader(r io.Reader) *TSVReader {
	return &TSVReader{
		r: bufio.NewReader(r),
	}
}

// Next reads the next line in the file, returning true if a valid row was read.
// Be sure to check Error() after this returns false, and do not continue to
// call after this returns false.
func (t *TSVReader) Next() bool {
	line, err := t.r.ReadString('\n')
	if err != nil {
		t.handleReadError(err, line)

		return false
	}

	line = strings.TrimSuffix(line, "\n")

	if line == "" || line[0] == '#' {
		return t.Next()
	}

	for i := numCols - 1; i > 0; i-- {
		line, t.columns[i] = splitLastTab(line)
	}

	t.columns[0] = line

	return t.validate()
}

func (t *TSVReader) handleReadError(err error, line string) {
	if !errors.Is(err, io.EOF) {
		t.err = err
	} else if line != "" {
		t.err = io.ErrUnexpectedEOF
	}
}

func splitLastTab(str string) (string, string) {
	pos := strings.LastIndexByte(str, '\t')

	if pos == -1 {
		return "", str
	}

	return str[:pos], str[pos+1:]
}

func (t *TSVReader) validate() bool {
	if !validateName(t.columns[1]) || !validateName(t.columns[2]) {
		t.err = errInvalidFormat

		return false
	}

	if !permsRE.MatchString(t.columns[3]) || !permsRE.MatchString(t.columns[4]) {
		t.err = errInvalidFormat

		return false
	}

	if !validatePermMatching(t.columns[3]) || !validatePermMatching(t.columns[4]) {
		t.err = errInvalidFormat

		return false
	}

	return true
}

func validateName(name string) bool {
	return nameRE.MatchString(name) || name == "*" || name == "^"
}

func validatePermMatching(perms string) bool {
	for i := 0; i < 3; i++ {
		set := 0

		for j := i; j < 9; j += 3 {
			if perms[j] == '^' {
				set++
			}
		}

		if set == 1 {
			return false
		}
	}

	return true
}

// Columns returns the columns of the row read after calling Next().
func (t *TSVReader) Columns() []string {
	return append([]string{}, t.columns[:]...)
}

// Error returns any error encountered during Next() parsing. Does not generate
// an error at end of file.
func (t *TSVReader) Error() error {
	return t.err
}
