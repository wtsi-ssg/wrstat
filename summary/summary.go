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
	timeMap := map[int64][]*int64{
		2628000:   {&s.filesizeA1m, &s.filesizeM1m},
		5256000:   {&s.filesizeA2m, &s.filesizeM2m},
		15768000:  {&s.filesizeA6m, &s.filesizeM6m},
		31557600:  {&s.filesizeA1y, &s.filesizeM1y},
		63115200:  {&s.filesizeA2y, &s.filesizeM2y},
		94672800:  {&s.filesizeA3y, &s.filesizeM3y},
		157788000: {&s.filesizeA5y, &s.filesizeM5y},
		220903200: {&s.filesizeA7y, &s.filesizeM7y},
	}

	keys := []int64{2628000, 5256000, 15768000, 31557600, 63115200, 94672800, 157788000, 220903200}

	if atime != 0 {
		setATimes(size, atime, keys, timeMap)
	}

	if mtime != 0 {
		setMTimes(size, mtime, keys, timeMap)

	}
}

func setATimes(size, time int64, keys []int64, timeMap map[int64][]*int64) {
	for _, v := range keys {
		if time < v {
			break
		}

		*timeMap[v][0] += size
	}
}

func setMTimes(size, time int64, keys []int64, timeMap map[int64][]*int64) {
	for _, v := range keys {
		if time < v {
			break
		}

		*timeMap[v][1] += size
	}
}

func (s *summaryWithTimes) updateATime(atime int64) {
	s.atime = atime
	s.resetATimes()

	s.setTimes(s.size, atime, 0)
}

func (s *summaryWithTimes) resetATimes() {
	for _, v := range []*int64{&s.filesizeA7y, &s.filesizeA5y, &s.filesizeA3y, &s.filesizeA2y,
		&s.filesizeA1y, &s.filesizeA6m, &s.filesizeA2m, &s.filesizeA1m} {
		*v = 0
	}
}
