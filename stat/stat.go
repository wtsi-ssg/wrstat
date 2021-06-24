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

const lstatSlowErr = Error("lstat exceeded timeout")

// Statter is something you use to get stats of files on disk. NB: this is NOT
// thread safe; you should only call Lstat() one at a time.
type Statter struct {
	timeout         time.Duration
	maxAttempts     int
	currentAttempts int
	logger          log15.Logger
}

// WithTimeout returns a Statter with the given timeout and maxAttempts
// configured. Timeouts are logged with the given logger.
func WithTimeout(timeout time.Duration, maxAttempts int, logger log15.Logger) *Statter {
	return &Statter{
		timeout:     timeout,
		maxAttempts: maxAttempts,
		logger:      logger,
	}
}

// Lstat calls os.Lstat() on the given path, but times it out after out
// configured timeout, retrying until we've hit our maxAttempts. NB: this is NOT
// thread safe, don't call this concurrently.
func (s *Statter) Lstat(path string) (info fs.FileInfo, err error) {
	infoCh := make(chan fs.FileInfo, 1)
	errCh := make(chan error, 1)
	s.currentAttempts++

	go func() {
		linfo, lerr := os.Lstat(path)
		infoCh <- linfo
		errCh <- lerr
	}()

	select {
	case err = <-errCh:
		info = <-infoCh

		return
	case <-time.After(s.timeout):
		if s.currentAttempts <= s.maxAttempts {
			s.logger.Warn("an lstat call exceeded timeout, will retry", "path", path, "attempts", s.currentAttempts)

			return s.Lstat(path)
		}

		s.logger.Warn("an lstat call exceeded timeout, giving up", "path", path, "attempts", s.currentAttempts)

		err = lstatSlowErr

		return
	}
}
