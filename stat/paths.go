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
	"bufio"
	"io"
	"io/fs"
	"strconv"
	"sync"
	"time"

	"github.com/inconshreveable/log15"
	"github.com/wtsi-ssg/wrstat/v5/reporter"
)

const lstatOpName = "lstat"

const errReservedOpName = Error("reserved operation name")

// Operation is a callback that once added to a Paths will be called on each
// path encountered. It receives the absolute path to the filesystem entry, and
// the FileInfo returned by Statter.Lstat() on that path.
type Operation func(absPath string, info fs.FileInfo) error

// Paths lets you get stats and carry out operations on those stats for many
// file paths.
type Paths struct {
	statter         Statter
	logger          log15.Logger
	reportFrequency time.Duration
	ops             map[string]Operation
	reporters       map[string]*reporter.Reporter
}

// NewPaths returns a Paths that will use the given Statter to do the Lstat
// calls and log issues to the logger. If you supply a reportFrequency greater
// than 0, then timings for the lstats and your operations will also be logged.
func NewPaths(statter Statter, logger log15.Logger, reportFrequency time.Duration) *Paths {
	return &Paths{
		statter:         statter,
		logger:          logger,
		reportFrequency: reportFrequency,
		ops:             make(map[string]Operation),
		reporters:       make(map[string]*reporter.Reporter),
	}
}

// AddOperation adds the given Operation callback so that when you Scan(), your
// callback will be called for each path scanned. You give the operation a name
// so that timings can be reported for each operation.
//
// You can't use the name "lstat", since that is used for reporting the Lstat
// timings.
func (p *Paths) AddOperation(name string, op Operation) error {
	if name == lstatOpName {
		return errReservedOpName
	}

	p.ops[name] = op
	p.reporters[name] = reporter.New(name, p.logger)

	return nil
}

// Scan scans through the given reader which should consist of quoted absolute
// file path per line. It calls our Statter.Lstat() on each, and passes the
// absolute path and FileInfo to any Operation callbacks you've added.
//
// Operations are run concurrently (so should not do something like write to the
// same file) and their errors logged, but otherwise ignored.
//
// We wait for all operations to complete before they are all called again, so
// it is safe to do something like write stat details to a file.
func (p *Paths) Scan(paths io.Reader) error {
	scanner := bufio.NewScanner(paths)

	r := reporter.New(lstatOpName, p.logger)
	p.reporters[lstatOpName] = r
	p.startReporting()

	var wg sync.WaitGroup

	for scanner.Scan() {
		path, err := strconv.Unquote(scanner.Text())
		if err != nil {
			return err
		}

		info, err := p.timeLstat(r, path)

		wg.Wait()

		if err != nil {
			continue
		}

		p.dispatch(path, info, &wg)
	}

	wg.Wait()
	p.stopReporting()

	return scanner.Err()
}

// startReporting calls StartReproting on all our reporters.
func (p *Paths) startReporting() {
	if p.reportFrequency <= 0 {
		return
	}

	for _, r := range p.reporters {
		r.StartReporting(p.reportFrequency)
	}
}

// timeLstat calls our statter.Lstat within a Reporter TimeOperation.
func (p *Paths) timeLstat(r *reporter.Reporter, path string) (info fs.FileInfo, err error) {
	err = r.TimeOperation(func() error {
		var lerr error
		info, lerr = p.statter.Lstat(path)

		return lerr
	})

	return
}

// dispatch gives absPath and info to each added Operation.
func (p *Paths) dispatch(absPath string, info fs.FileInfo, wg *sync.WaitGroup) {
	for name, op := range p.ops {
		wg.Add(1)

		go func(name string, op Operation, absPath string, info fs.FileInfo) {
			defer wg.Done()

			r := p.reporters[name]
			if err := r.TimeOperation(func() error {
				return op(absPath, info)
			}); err != nil {
				p.logger.Warn("operation error", "op", name, "err", err)
			}
		}(name, op, absPath, info)
	}
}

// stopReporting calls StopReproting on all our reporters.
func (p *Paths) stopReporting() {
	if p.reportFrequency <= 0 {
		return
	}

	for _, r := range p.reporters {
		r.StopReporting()
	}
}
