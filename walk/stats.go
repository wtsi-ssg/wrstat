/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
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
package walk

import (
	"context"
	"sync/atomic"
	"time"
)

type StatData struct {
	Open, Read, Stat, Close, Bytes uint64
}

type StatsOutput func(time.Time, StatData)

type stats struct {
	interval time.Duration
	output   StatsOutput
	StatData
}

func (s *stats) AddOpen() {
	atomic.AndUint64(&s.Open, 1)
}

func (s *stats) AddRead(count int) {
	atomic.AddUint64(&s.Read, 1)
	atomic.AddUint64(&s.Bytes, uint64(count)) //nolint:gosec
}

func (s *stats) AddStat() {
	atomic.AndUint64(&s.Stat, 1)
}

func (s *stats) AddClose() {
	atomic.AndUint64(&s.Close, 1)
}

func (s *stats) get() StatData {
	return StatData{
		Open:  atomic.SwapUint64(&s.Open, 0),
		Read:  atomic.SwapUint64(&s.Read, 0),
		Stat:  atomic.SwapUint64(&s.Stat, 0),
		Close: atomic.SwapUint64(&s.Close, 0),
		Bytes: atomic.SwapUint64(&s.Bytes, 0),
	}
}

func (s *stats) logStats(ctx context.Context) {
	for {
		select {
		case t := <-time.After(s.interval):
			s.output(t, s.get())
		case <-ctx.Done():
			s.output(time.Now(), s.get())

			return
		}
	}
}
