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
	"errors"
	"io"
	"io/fs"
	"strconv"
	"sync"
	"time"

	"github.com/inconshreveable/log15"
	"github.com/wtsi-ssg/wrstat/v6/reporter"
)

const lstatOpName = "lstat"

const (
	errReservedOpName = Error("reserved operation name")
	errScanTimeout    = Error("scan took too long")
)

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
	ScanTimeout     time.Duration
	reporters       map[string]*reporter.Reporter
}

type PathsConfig struct {
	Logger          log15.Logger
	ReportFrequency time.Duration
	ScanTimeout     time.Duration
}

// NewPaths returns a Paths that will use the given Statter to do the Lstat
// calls and log issues to any configured logger. If you configure a
// reportFrequency greater than 0, then timings for the lstats and your
// operations will also be logged. You can also configure a MaxTime that a
// Scan() can run for before it fails.
func NewPaths(statter Statter, pathsConfig PathsConfig) *Paths {
	return &Paths{
		statter:         statter,
		logger:          pathsConfig.Logger,
		reportFrequency: pathsConfig.ReportFrequency,
		ScanTimeout:     pathsConfig.ScanTimeout,
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
// absolute path and FileInfo to any Operation callbacks you've added. Errors
// from the Statter are normally ignored, with the exeption of
// StatterWithTimeout's failure due to too many consecutive timeouts in a row.
//
// Operations are run concurrently (so should not do something like write to the
// same file) and their errors logged, but otherwise ignored.
//
// We wait for all operations to complete before they are all called again, so
// it is safe to do something like write stat details to a file.
//
// If a MaxTime has been configured, Scan() will stop and return an error as
// soon as that amount of time has passed.
func (p *Paths) Scan(paths io.Reader) error {
	scanner := bufio.NewScanner(paths)

	r := reporter.New(lstatOpName, p.logger)
	p.reporters[lstatOpName] = r
	p.startReporting()

	endTime := time.Now().Add(p.ScanTimeout)

	err := p.lstatEachPath(scanner, r, endTime)
	if err != nil {
		return err
	}

	return scanner.Err()
}

func (p *Paths) lstatEachPath(scanner *bufio.Scanner, r *reporter.Reporter, //nolint:funlen,gocognit
	endTime time.Time) (err error) {
	var wg sync.WaitGroup
	defer func() {
		errw := p.waitUntilWGOrMaxTime(&wg, endTime)
		if errw != nil {
			err = errw
		}

		p.stopReporting()
	}()

	for scanner.Scan() {
		path, erru := strconv.Unquote(scanner.Text())
		if erru != nil {
			return erru
		}

		info, errt := p.timeLstat(r, path)

		errWg := p.waitUntilWGOrMaxTime(&wg, endTime)
		if errWg != nil {
			return errWg
		}

		if errors.Is(errt, errLstatConsecFails) {
			return errt
		} else if errt != nil {
			continue
		}

		p.dispatch(path, info, &wg)
	}

	return err
}

func (p *Paths) waitUntilWGOrMaxTime(wg *sync.WaitGroup, endTime time.Time) error {
	if p.ScanTimeout == 0 {
		wg.Wait()

		return nil
	}

	waitCh := make(chan bool)
	timer := time.NewTimer(time.Until(endTime))

	go func() {
		wg.Wait()
		close(waitCh)
	}()

	select {
	case <-timer.C:
		return errScanTimeout
	case <-waitCh:
		return nil
	}
}

// startReporting calls StartReporting on all our reporters.
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
