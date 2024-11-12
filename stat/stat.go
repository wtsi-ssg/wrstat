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

package stat

import (
	"io/fs"
	"os"
	"syscall"
	"time"

	"github.com/inconshreveable/log15"
)

type Error string

func (e Error) Error() string { return string(e) }

const (
	errLstatSlow        = Error("lstat exceeded timeout")
	errLstatConsecFails = Error("lstat failed too many times in a row")
	minimumDate         = 315532801 // 1980-01-01T00:00:01+00
)

// Statter is something you use to get stats of files on disk.
type Statter interface {
	// Lstat calls os.Lstat() on the given path, returning the FileInfo.
	Lstat(path string) (info fs.FileInfo, err error)
}

// LstatFunc matches the signature of os.Lstat.
type LstatFunc func(string) (fs.FileInfo, error)

// StatterWithTimeout is a Statter implementation. NB: this is NOT thread safe;
// you should only call Lstat() one at a time.
type StatterWithTimeout struct {
	timeout         time.Duration
	maxAttempts     int
	currentAttempts int
	maxFailureCount int
	failureCount    int
	lstat           LstatFunc
	logger          log15.Logger
	defTime         int64
}

// WithTimeout returns a Statter with the given timeout, maxAttempts and
// maxFailureCount configured. Timeouts are logged with the given logger.
//
// Timeouts on single files do not result in an error, but timeouts of
// maxFailureCount consecutive files does.
func WithTimeout(timeout time.Duration, maxAttempts, maxFailureCount int, logger log15.Logger) *StatterWithTimeout {
	return &StatterWithTimeout{
		timeout:         timeout,
		maxAttempts:     maxAttempts,
		logger:          logger,
		maxFailureCount: maxFailureCount,
		lstat:           os.Lstat,
		defTime:         time.Now().Unix(),
	}
}

// SetLstat can be used when testing when you need to mock actual Lstat calls.
// The lstat defaults to os.Lstat.
func (s *StatterWithTimeout) SetLstat(lstat LstatFunc) {
	s.lstat = lstat
}

// Lstat calls os.Lstat() on the given path, but times it out after our
// configured timeout, retrying until we've hit our maxAttempts. NB: this is NOT
// thread safe, don't call this concurrently.
func (s *StatterWithTimeout) Lstat(path string) (info fs.FileInfo, err error) {
	infoCh := make(chan fs.FileInfo, 1)
	errCh := make(chan error, 1)
	s.currentAttempts++

	timer := time.NewTimer(s.timeout)

	go s.doLstat(path, infoCh, errCh)

	select {
	case err = <-errCh:
		info = <-infoCh
		s.currentAttempts = 0
		s.failureCount = 0

		timer.Stop()

		return info, err
	case <-timer.C:
		if s.currentAttempts <= s.maxAttempts {
			s.logger.Warn("an lstat call exceeded timeout, will retry", "path", path, "attempts", s.currentAttempts)

			return s.Lstat(path)
		}

		s.logger.Warn("an lstat call exceeded timeout, giving up", "path", path, "attempts", s.currentAttempts)

		s.currentAttempts = 0
		err = errLstatSlow

		s.failureCount++
		if s.failureCount < s.maxFailureCount {
			return info, err
		}

		s.logger.Error("too many lstat calls failed consecutively, terminating", "failures", s.failureCount)

		return info, errLstatConsecFails
	}
}

// doLstat does the actual Lstat call and sends results on the given channels.
func (s *StatterWithTimeout) doLstat(path string, infoCh chan fs.FileInfo, errCh chan error) {
	info, err := s.lstat(path)
	if err == nil {
		stat, ok := info.Sys().(*syscall.Stat_t)
		if ok {
			s.correctFutureTimes(stat)
			s.correctZeroTimes(stat)
		}
	}

	infoCh <- info
	errCh <- err
}

func (s *StatterWithTimeout) correctFutureTimes(stat *syscall.Stat_t) {
	if stat.Atim.Sec > s.defTime {
		stat.Atim.Sec = s.defTime
	}

	if stat.Mtim.Sec > s.defTime {
		stat.Mtim.Sec = s.defTime
	}
}

func (s *StatterWithTimeout) correctZeroTimes(stat *syscall.Stat_t) {
	if stat.Atim.Sec <= minimumDate {
		stat.Atim.Sec = s.correctZeroTime(stat)
	}

	if stat.Mtim.Sec <= minimumDate {
		stat.Mtim.Sec = s.correctZeroTime(stat)
	}
}

func (s *StatterWithTimeout) correctZeroTime(stat *syscall.Stat_t) int64 {
	switch {
	case stat.Mtim.Sec > minimumDate:
		return stat.Mtim.Sec
	case stat.Atim.Sec > minimumDate:
		return stat.Atim.Sec
	case stat.Ctim.Sec > minimumDate:
		return stat.Ctim.Sec
	default:
		return s.defTime
	}
}
