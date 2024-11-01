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
	"time"

	"github.com/inconshreveable/log15"
)

type Error string

func (e Error) Error() string { return string(e) }

const (
	errLstatSlow        = Error("lstat exceeded timeout")
	errLstatConsecFails = Error("lstat failed too many times in a row")
)

// Statter is something you use to get stats of files on disk.
type Statter interface {
	// Lstat calls os.Lstat() on the given path, returning the FileInfo.
	Lstat(path string) (info fs.FileInfo, err error)
}

// StatterWithTimeout is a Statter implementation. NB: this is NOT thread safe;
// you should only call Lstat() one at a time.
type StatterWithTimeout struct {
	timeout         time.Duration
	maxAttempts     int
	currentAttempts int
	maxFailureCount int
	failureCount    int
	logger          log15.Logger
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
	}
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

		s.logger.Warn("too many lstat calls failed consecutively, terminating", "failures", s.failureCount)

		return info, errLstatConsecFails
	}
}

// doLstat does the actual Lstat call and sends results on the given channels.
func (s *StatterWithTimeout) doLstat(path string, infoCh chan fs.FileInfo, errCh chan error) {
	if os.Getenv("WRSTAT_TEST_LSTAT") != "" {
		<-time.After(1 * time.Millisecond)
	}

	info, err := os.Lstat(path)
	infoCh <- info
	errCh <- err
}
