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
	refTime     time.Time
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
// mtime if it is newer than our current one. It also sets the various
// filesize[AM][timeperiod] properties based on the age.
func (s *summaryWithTimes) add(size int64, atime int64, mtime int64) {
	s.summary.add(size)

	s.setAgeSizes(size, atime, mtime)

	if atime > 0 && (s.atime == 0 || atime < s.atime) {
		s.atime = atime
	}

	if mtime > 0 && (s.mtime == 0 || mtime > s.mtime) {
		s.mtime = mtime
	}
}

func (s *summaryWithTimes) setAgeSizes(size, atime, mtime int64) {

	if atime != 0 {
		s.setAtimeSizes(size, time.Unix(atime, 0))
	}

	if mtime != 0 {
		s.setMtimeSizes(size, time.Unix(mtime, 0))
	}
}

func (s *summaryWithTimes) setAtimeSizes(size int64, t time.Time) {
	if t.After(s.refTime.AddDate(0, -1, 0)) {
		return
	}

	s.filesizeA1m += size

	if t.After(s.refTime.AddDate(0, -2, 0)) {
		return
	}

	s.filesizeA2m += size

	if t.After(s.refTime.AddDate(0, -6, 0)) {
		return
	}

	s.filesizeA6m += size

	if t.After(s.refTime.AddDate(-1, 0, 0)) {
		return
	}

	s.filesizeA1y += size

	if t.After(s.refTime.AddDate(-2, 0, 0)) {
		return
	}

	s.filesizeA2y += size

	if t.After(s.refTime.AddDate(-3, 0, 0)) {
		return
	}

	s.filesizeA3y += size

	if t.After(s.refTime.AddDate(-5, 0, 0)) {
		return
	}

	s.filesizeA5y += size

	if t.After(s.refTime.AddDate(-7, 0, 0)) {
		return
	}

	s.filesizeA7y += size
}

func (s *summaryWithTimes) setMtimeSizes(size int64, t time.Time) {
	switch {
	case t.Before(s.refTime.AddDate(0, -1, 0)):
		s.filesizeM1m += size
		fallthrough
	case t.Before(s.refTime.AddDate(0, -2, 0)):
		s.filesizeM2m += size
		fallthrough
	case t.Before(s.refTime.AddDate(0, -6, 0)):
		s.filesizeM6m += size
		fallthrough
	case t.Before(s.refTime.AddDate(-1, 0, 0)):
		s.filesizeM1y += size
		fallthrough
	case t.Before(s.refTime.AddDate(-2, 0, 0)):
		s.filesizeM2y += size
		fallthrough
	case t.Before(s.refTime.AddDate(-3, 0, 0)):
		s.filesizeM3y += size
		fallthrough
	case t.Before(s.refTime.AddDate(-5, 0, 0)):
		s.filesizeM5y += size
		fallthrough
	case t.Before(s.refTime.AddDate(-7, 0, 0)):
		//fmt.Println(t, " is before ", s.refTime.AddDate(-7, 0, 0))
		s.filesizeM7y += size
	}
}
