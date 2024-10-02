/*******************************************************************************
 * Copyright (c) 2021 Genome Research Ltd.
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

// package summary lets you summarise file stats.

package summary

import (
	"time"
)

const (
	secondsIn1m = 2628000
	secondsIn2m = 5256000
	secondsIn6m = 15768000
	secondsIn1y = 31557600
	secondsIn2y = 63115200
	secondsIn3y = 94672800
	secondsIn5y = 157788000
	secondsIn7y = 220903200
)

// summary holds count and size and lets you accumulate count and size as you
// add more things with a size.
type summary struct {
	count int64
	size  int64
}

// add will increment our count and add the given size to our size.
func (s *summary) add(size int64) {
	s.count++
	s.size += size
}

// summaryWithTimes is like summary, but also holds the oldest atime and
// newest mtime add()ed.
type summaryWithTimes struct {
	summary
	atime       int64 // seconds since Unix epoch
	mtime       int64 // seconds since Unix epoch
	filesizeA7y int64
	filesizeA5y int64
	filesizeA3y int64
	filesizeA2y int64
	filesizeA1y int64
	filesizeA6m int64
	filesizeA2m int64
	filesizeA1m int64
	filesizeM7y int64
	filesizeM5y int64
	filesizeM3y int64
	filesizeM2y int64
	filesizeM1y int64
	filesizeM6m int64
	filesizeM2m int64
	filesizeM1m int64
}

// add will increment our count and add the given size to our size. It also
// stores the given atime if it is older than our current one, and the given
// mtime if it is newer than our current one.
func (s *summaryWithTimes) add(size int64, atime int64, mtime int64) {
	s.summary.add(size)

	s.setTimes(size, atime, mtime)

	if atime > 0 && (s.atime == 0 || atime < s.atime) {
		s.atime = atime
	}

	if mtime > 0 && (s.mtime == 0 || mtime > s.mtime) {
		s.mtime = mtime
	}
}

func (s *summaryWithTimes) setTimes(size, atime, mtime int64) {
	curTime := time.Now().Unix()

	aToSet := []*int64{&s.filesizeA1m, &s.filesizeA2m, &s.filesizeA6m, &s.filesizeA1y,
		&s.filesizeA2y, &s.filesizeA3y, &s.filesizeA5y, &s.filesizeA7y}
	mToSet := []*int64{&s.filesizeM1m, &s.filesizeM2m, &s.filesizeM6m, &s.filesizeM1y,
		&s.filesizeM2y, &s.filesizeM3y, &s.filesizeM5y, &s.filesizeM7y}

	if atime != 0 {
		setIntervals(getNumOfSatisfyingIntervals(atime, curTime, len(aToSet)), size, aToSet)
	}

	if mtime != 0 {
		setIntervals(getNumOfSatisfyingIntervals(mtime, curTime, len(mToSet)), size, mToSet)
	}
}

func getNumOfSatisfyingIntervals(amTime, curTime int64, toSet int) int {
	switch {
	case amTime < curTime-secondsIn7y:
		return toSet
	case amTime < curTime-secondsIn5y:
		return toSet - 1
	case amTime < curTime-secondsIn3y:
		return toSet - 2
	case amTime < curTime-secondsIn2y:
		return toSet - 3
	case amTime < curTime-secondsIn1y:
		return toSet - 4
	case amTime < curTime-secondsIn6m:
		return toSet - 5
	case amTime < curTime-secondsIn2m:
		return toSet - 6
	case amTime < curTime-secondsIn1m:
		return toSet - 7
	default:
		return 0
	}
}

func setIntervals(numIntervals int, size int64, toSet []*int64) {
	for i := 0; i <= numIntervals-1; i++ {
		*toSet[i] += size
	}
}
