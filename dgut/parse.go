/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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

// package dgut lets you create and query a database made from dgut files.

package dgut

import (
	"bufio"
	"io"
	"strconv"
	"strings"

	"github.com/wtsi-ssg/wrstat/summary"
)

type Error string

func (e Error) Error() string { return string(e) }

const ErrInvalidFormat = Error("the provided data was not in dgut format")

const (
	gutDataCols = 6
)

type dgutParserCallBack func(*DGUT)

// parseDGUTLines will parse the given dgut file data (as output by
// summary.DirGroupUserType.Output()) and send *DGUT structs to your callback.
//
// Each *DGUT will correspond to one of the directories in your dgut file data,
// and contain all the *GUT information for that directory. Your callback will
// receive exactly 1 *DGUT per unique directory. (This relies on the dgut file
// data being sorted, as it normally would be.)
//
// Any issues with parsing the dgut file data will result in this method
// returning an error.
func parseDGUTLines(data io.Reader, cb dgutParserCallBack) error {
	dgut, guts := &DGUT{}, []*GUT{}

	scanner := bufio.NewScanner(data)
	for scanner.Scan() {
		thisDir, g, err := parseDGUTLine(scanner.Text())
		if err != nil {
			return err
		}

		if thisDir != dgut.Dir {
			if dgut.Dir != "" {
				dgut.GUTs = guts
				cb(dgut)
			}

			dgut, guts = &DGUT{Dir: thisDir}, []*GUT{}
		}

		guts = append(guts, g)
	}

	if dgut.Dir != "" {
		dgut.GUTs = guts
		cb(dgut)
	}

	return scanner.Err()
}

// parseDGUTLine parses a line of summary.DirGroupUserType.Output() into a
// directory string and a *dgut for the other information.
//
// Returns an error if line didn't have the expected format.
func parseDGUTLine(line string) (string, *GUT, error) {
	parts, err := splitDGUTLine(line)
	if err != nil {
		return "", nil, err
	}

	ints, err := gutLinePartsToInts(parts)
	if err != nil {
		return "", nil, err
	}

	return parts[0], &GUT{
		GID:   uint32(ints[0]),
		UID:   uint32(ints[1]),
		FT:    summary.DirGUTFileType(ints[2]),
		Count: ints[3],
		Size:  ints[4],
	}, nil
}

// splitDGUTLine trims the \n from line and splits it in to 6 columns.
func splitDGUTLine(line string) ([]string, error) {
	line = strings.TrimSuffix(line, "\n")

	parts := strings.Split(line, "\t")
	if len(parts) != gutDataCols {
		return nil, ErrInvalidFormat
	}

	return parts, nil
}

// gutLinePartsToInts takes the output of splitDGUTLine() and returns the last
// 6 columns as ints.
func gutLinePartsToInts(parts []string) ([]uint64, error) {
	ints := make([]uint64, 5)

	var err error

	if ints[0], err = strconv.ParseUint(parts[1], 10, 32); err != nil {
		return nil, ErrInvalidFormat
	}

	if ints[1], err = strconv.ParseUint(parts[2], 10, 32); err != nil {
		return nil, ErrInvalidFormat
	}

	if ints[2], err = strconv.ParseUint(parts[3], 10, 8); err != nil {
		return nil, ErrInvalidFormat
	}

	if ints[3], err = strconv.ParseUint(parts[4], 10, 64); err != nil {
		return nil, ErrInvalidFormat
	}

	if ints[4], err = strconv.ParseUint(parts[5], 10, 64); err != nil {
		return nil, ErrInvalidFormat
	}

	return ints, nil
}
