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

// package reporter is used to report on timings of operations.

package reporter

import (
	"fmt"
	"sync"
	"time"

	"github.com/inconshreveable/log15"
)

const nanosecondsInSecond = 1000000000

// Reporter can be used to output timing information on how long something is
// taking.
type Reporter struct {
	operation       string       // the name of the operation you will Time(), output in Report().
	logger          log15.Logger // where your reports will be logged to.
	currentDuration time.Duration
	totalDuration   time.Duration
	failedDuration  time.Duration
	currentCount    int64
	totalCount      int64
	failedCount     int64
	enabled         bool
	started         bool
	stopCh          chan struct{}
	doneCh          chan struct{}
	sync.Mutex
}

// New returns a reporter that will log how long operation took to logger.
func New(operation string, logger log15.Logger) *Reporter {
	return &Reporter{
		operation: operation,
		logger:    logger,
	}
}

// Enable will cause future TimeOperation() calls to time the operation. This is
// so that if not enabled, you can have TimeOperation() calls throughout your
// code and it won't be expensive since they will do nothing until you chose to
// Enable() the reporter. NB: this is NOT thread safe.
func (r *Reporter) Enable() {
	r.enabled = true
}

// TimeOperation, if Enable() has not yet been called, will simply call your
// given func and return its error. If Enable() has been called, it will time
// how long your func takes to run, so that Report() can report details about
// your func.
func (r *Reporter) TimeOperation(f func() error) error {
	if !r.enabled {
		return f()
	}

	t := time.Now()
	err := f()
	d := time.Since(t)

	r.Lock()
	defer r.Unlock()

	if err != nil {
		r.failedCount++
		r.failedDuration += d
	} else {
		r.currentCount++
		r.currentDuration += d
	}

	return err
}

// Report outputs timings collected since the last Report() call. Operations
// that returned an error are not included in these reports.
func (r *Reporter) Report() {
	r.Lock()
	defer r.Unlock()

	r.logger.Info("report since last",
		"op", r.operation,
		"count", r.currentCount,
		"time", r.currentDuration,
		"ops/s", opsPerSecond(r.currentCount, r.currentDuration))

	r.totalCount += r.currentCount
	r.totalDuration += r.currentDuration
	r.currentCount = 0
	r.currentDuration = 0
}

// ReportFinal reports overall and failed timings.
func (r *Reporter) ReportFinal() {
	r.logger.Info("report overall",
		"op", r.operation,
		"count", r.totalCount,
		"time", r.totalDuration,
		"ops/s", opsPerSecond(r.totalCount, r.totalDuration))

	if r.failedCount > 0 {
		r.logger.Warn("report failed",
			"op", r.operation,
			"count", r.failedCount,
			"time", r.failedDuration,
			"ops/s", opsPerSecond(r.failedCount, r.failedDuration))
	}
}

// opsPerSecond returns operations/d.Seconds rounded to 2 decimal places, or n/a
// if either is 0.
func opsPerSecond(ops int64, d time.Duration) string {
	if ops == 0 || d == 0 {
		return "n/a"
	}

	return fmt.Sprintf("%.2f", float64(ops)/float64(d.Nanoseconds())*nanosecondsInSecond)
}

// StartReporting calls Enable() and then Report() regularly every frequency.
// NB: this is NOT thread safe.
func (r *Reporter) StartReporting(frequency time.Duration) {
	r.Enable()

	r.started = true
	r.stopCh = make(chan struct{})
	r.doneCh = make(chan struct{})
	ticker := time.NewTicker(frequency)

	go func() {
		for {
			select {
			case <-ticker.C:
				r.Report()
			case <-r.stopCh:
				ticker.Stop()
				r.Report()
				r.ReportFinal()
				close(r.doneCh)

				return
			}
		}
	}()
}

// StopReporting stops the regular calling of Report() and triggers
// ReportFinal().
func (r *Reporter) StopReporting() {
	if !r.started {
		return
	}

	close(r.stopCh)
	<-r.doneCh

	r.started = false
}
