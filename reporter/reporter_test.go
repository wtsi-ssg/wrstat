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

package reporter

import (
	"bytes"
	"regexp"
	"testing"
	"time"

	"github.com/inconshreveable/log15"
	. "github.com/smartystreets/goconvey/convey"
)

type Error string

func (e Error) Error() string { return string(e) }

const errTest = Error("test error")

func TestReporter(t *testing.T) {
	var opErr error

	op := func() error {
		<-time.After(5 * time.Millisecond)

		return opErr
	}

	Convey("Given a Reporter", t, func() {
		buff := new(bytes.Buffer)
		l := log15.New()
		l.SetHandler(log15.StreamHandler(buff, log15.LogfmtFormat()))

		r := New("foo", l)
		So(r, ShouldNotBeNil)

		Convey("You can carry out operations", func() {
			err := r.TimeOperation(op)
			So(err, ShouldBeNil)
			r.Report()
			So(buff.String(), ShouldContainSubstring, `lvl=info msg="report since last" op=foo count=0 time=0s ops/s=n/a`)

			Convey("Once enabled, you can time operations", func() {
				r.Enable()
				buff.Reset()

				err := r.TimeOperation(op)
				So(err, ShouldBeNil)
				err = r.TimeOperation(op)
				So(err, ShouldBeNil)
				opErr = errTest
				err = r.TimeOperation(op)
				So(err, ShouldNotBeNil)
				opErr = nil

				r.Report()
				So(buff.String(), ShouldContainSubstring, `lvl=info msg="report since last" op=foo count=2`)
				So(buff.String(), ShouldContainSubstring, `time=`)
				So(buff.String(), ShouldNotContainSubstring, `ops/s=n/a`)

				buff.Reset()
				r.ReportFinal()
				So(buff.String(), ShouldContainSubstring, `lvl=info msg="report overall" op=foo count=2`)
				So(buff.String(), ShouldNotContainSubstring, `ops/s=n/a`)
				So(buff.String(), ShouldContainSubstring, `lvl=warn msg="report failed" op=foo count=1`)
			})
		})

		Convey("You can report operation timings regularly", func() {
			r.StartReporting(13 * time.Millisecond)
			for i := 0; i < 6; i++ {
				err := r.TimeOperation(op)
				So(err, ShouldBeNil)
			}

			r.StopReporting()
			So(buff.String(), ShouldContainSubstring, `lvl=info msg="report since last" op=foo count=2`)
			reg := regexp.MustCompile("report since last")
			matches := reg.FindAllStringIndex(buff.String(), -1)
			So(len(matches), ShouldBeBetweenOrEqual, 2, 5)
			So(buff.String(), ShouldContainSubstring, `lvl=info msg="report overall" op=foo count=6`)
			So(buff.String(), ShouldNotContainSubstring, `lvl=warn msg="report failed"`)

			buff.Reset()
			r.StopReporting()
			So(buff.String(), ShouldBeEmpty)
		})
	})
}
