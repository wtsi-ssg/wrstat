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
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

const (
	errTestFail        = Error("test fail")
	errTestFileDetails = Error("file details wrong")
)

func TestPaths(t *testing.T) {
	statterTimeout := 1 * time.Second
	statterRetries := 2
	statterConsecutiveFails := 2

	Convey("Given a Paths with a report frequency", t, func() {
		buff, l := newLogger()
		s := WithTimeout(statterTimeout, statterRetries, statterConsecutiveFails, l)
		p := NewPaths(s, l, 15*time.Millisecond)
		So(p, ShouldNotBeNil)

		Convey("You can't add an operation with the reserved name", func() {
			err := p.AddOperation("lstat", func(string, fs.FileInfo) error {
				return nil
			})
			So(err, ShouldNotBeNil)
			So(err, ShouldEqual, errReservedOpName)
		})

		Convey("You can add operations", func() {
			sleepN, failN := addTestOperations(p)

			Convey("Which get called when you Scan, providing timing reports", func() {
				r := createScanInput(t)
				err := p.Scan(r)
				So(err, ShouldBeNil)

				So(*sleepN, ShouldEqual, 3)
				So(buff.String(), ShouldContainSubstring, `lvl=info msg="report since last" op=check count=`)
				So(buff.String(), ShouldContainSubstring, `lvl=info msg="report overall" op=check count=3`)
				So(buff.String(), ShouldNotContainSubstring, `file details wrong`)

				So(*failN, ShouldEqual, 3)
				So(buff.String(), ShouldContainSubstring, `lvl=warn msg="operation error" op=fail err="test fail"`)
				So(buff.String(), ShouldContainSubstring, `lvl=info msg="report since last" op=fail count=0 time=0s ops/s=n/a`)
				So(buff.String(), ShouldContainSubstring, `lvl=info msg="report overall" op=fail count=0 time=0s ops/s=n/a`)
				So(buff.String(), ShouldContainSubstring, `lvl=warn msg="report failed" op=fail count=3`)

				So(buff.String(), ShouldContainSubstring, `lvl=info msg="report since last" op=lstat count=`)
				So(buff.String(), ShouldContainSubstring, `lvl=info msg="report overall" op=lstat count=3`)
				So(buff.String(), ShouldContainSubstring, `lvl=warn msg="report failed" op=lstat count=2`)
			})
		})

		Convey("Given a small max failure count, scan fails with consecutive failures", func() {
			s = WithTimeout(1*time.Nanosecond, statterRetries, 2, l)
			p = NewPaths(s, l, 15*time.Millisecond)
			So(p, ShouldNotBeNil)

			r := createScanInput(t)
			err := p.Scan(r)
			So(err, ShouldEqual, errLstatConsecFails)
		})

		Convey("Given a small max failure count, scan succeeds with non-consecutive failures", func() {
			s = WithTimeout(100*time.Millisecond, 1, 2, l)

			count := 0
			mockLstat := func(path string) (fs.FileInfo, error) {
				if count%2 != 0 {
					time.Sleep(200 * time.Millisecond)
				}

				count++

				return os.Lstat(path)
			}

			s.SetLstat(mockLstat)

			p = NewPaths(s, l, 15*time.Millisecond)
			So(p, ShouldNotBeNil)

			r := createScanInput(t)

			err := p.Scan(r)
			So(err, ShouldBeNil)
		})
	})

	Convey("Given a Paths with 0 report frequency", t, func() {
		buff, l := newLogger()
		s := WithTimeout(statterTimeout, statterRetries, statterConsecutiveFails, l)
		p := NewPaths(s, l, 0)
		So(p, ShouldNotBeNil)

		r := createScanInput(t)

		Convey("You can add operations and scan with no timing reports", func() {
			checkN, failN := addTestOperations(p)
			err := p.Scan(r)
			So(err, ShouldBeNil)

			So(*checkN, ShouldEqual, 3)
			So(*failN, ShouldEqual, 3)
			So(buff.String(), ShouldNotContainSubstring, `lvl=info msg="report`)
			So(buff.String(), ShouldContainSubstring, `lvl=warn msg="operation error" op=fail err="test fail"`)
		})

		Convey("Operations you add run concurrently during a scan", func() {
			addTestablyConcurrentOperations(p)
			err := p.Scan(r)
			So(err, ShouldBeNil)
			So(buff.String(), ShouldNotContainSubstring, `lvl=warn msg="operation error" op=first err="test fail"`)
			So(buff.String(), ShouldNotContainSubstring, `lvl=warn msg="operation error" op=second err="test fail"`)
		})

		Convey("Operations you add run concurrently with the next lstat", func() {
			addOperationConcurrentWithLstat(p)
			err := p.Scan(r)
			So(err, ShouldBeNil)
			So(buff.String(), ShouldNotContainSubstring, `lvl=warn msg="operation error" op=withlstat err="test fail"`)
		})

		Convey("FileOperation works as expected", func() {
			dir := t.TempDir()
			outPath := filepath.Join(dir, "out")
			out, err := os.Create(outPath)
			So(err, ShouldBeNil)

			err = p.AddOperation("file", FileOperation(out))
			So(err, ShouldBeNil)

			err = p.Scan(r)
			So(err, ShouldBeNil)

			err = out.Close()
			So(err, ShouldBeNil)
			output, err := os.ReadFile(outPath)
			So(err, ShouldBeNil)

			So(string(output), ShouldContainSubstring, "\t0\t")
			So(string(output), ShouldContainSubstring, "\t1\t")
			So(string(output), ShouldContainSubstring, "\tf\t")
		})
	})
}

// addTestOperations adds a "check" and a "fail" operation to the given Paths,
// and returns counters that tell you how many times each was called.
func addTestOperations(p *Paths) (*int, *int) {
	checkN := 0
	err := p.AddOperation("check", func(absPath string, info fs.FileInfo) error {
		<-time.After(5 * time.Millisecond)

		checkN++

		return checkFileDetails(absPath, info)
	})
	So(err, ShouldBeNil)

	failN := 0
	err = p.AddOperation("fail", func(string, fs.FileInfo) error {
		failN++

		return errTestFail
	})
	So(err, ShouldBeNil)

	So(len(p.ops), ShouldEqual, 2)
	So(len(p.reporters), ShouldEqual, 2)

	return &checkN, &failN
}

// checkFileDetails checks that we actually get expected file paths and info
// in our Operation callbacks.
func checkFileDetails(absPath string, info fs.FileInfo) error {
	switch {
	case strings.HasSuffix(absPath, "empty"):
		if info.Size() != 0 {
			return errTestFileDetails
		}
	case strings.HasSuffix(absPath, "content1"):
		if info.Size() != 1 {
			return errTestFileDetails
		}
	case strings.HasSuffix(absPath, "content2"):
		if info.Size() != 1 {
			return errTestFileDetails
		}
	default:
		return errTestFileDetails
	}

	return nil
}

// createScanInput creates 2 test files and an io.Reader of their paths, with
// a non-existent path in between.
func createScanInput(t *testing.T) io.Reader {
	t.Helper()

	pathEmpty, pathContent1, pathContent2 := createTestFiles(t)
	r := strings.NewReader(strconv.Quote(pathEmpty) + "\n" +
		strconv.Quote("/foo/bar") + "\n" + strconv.Quote(pathContent1) + "\n" +
		strconv.Quote("/foo/bar") + "\n" + strconv.Quote(pathContent2))

	return r
}

// addTestablyConcurrentOperations adds 2 operations "first" and "second" to p
// that would only work if they were running concurrently, not one after the
// other.
func addTestablyConcurrentOperations(p *Paths) {
	testCh := make(chan bool)
	err := p.AddOperation("first", func(string, fs.FileInfo) error {
		select {
		case <-time.After(1 * time.Second):
			return errTestFail
		case testCh <- true:
			return nil
		}
	})
	So(err, ShouldBeNil)

	err = p.AddOperation("second", func(string, fs.FileInfo) error {
		select {
		case <-time.After(1 * time.Second):
			return errTestFail
		case <-testCh:
			return nil
		}
	})
	So(err, ShouldBeNil)
}

// addOperationConcurrentWithLstat adds 1 operation "withlstat" that would only
// work if run concurrently with the following Lstat call. It also would fail if
// run concurrently with itself (ie. this tests that after the Lstat call, we
// wait for previous Operations to complete).
func addOperationConcurrentWithLstat(p *Paths) {
	testCh := make(chan bool)
	p.statter = &statterWithConcurrentTest{ch: testCh}

	var started, ended int32

	err := p.AddOperation("withlstat", func(absPath string, info fs.FileInfo) error {
		atomic.AddInt32(&started, 1)

		if atomic.LoadInt32(&started) != atomic.LoadInt32(&ended)+1 {
			return errTestFail
		}

		if atomic.LoadInt32(&started) == 3 {
			return nil
		}

		select {
		case <-time.After(1 * time.Second):
			return errTestFail
		case testCh <- true:
		}

		<-time.After(50 * time.Millisecond)
		atomic.AddInt32(&ended, 1)

		return nil
	})
	So(err, ShouldBeNil)
}

// statterWithConcurrentTest is used in addOperationConcurrentWithLstat() to
// enable that test.
type statterWithConcurrentTest struct {
	ch chan bool
	i  int
}

func (s *statterWithConcurrentTest) Lstat(_ string) (info fs.FileInfo, err error) {
	s.i++
	if s.i == 1 {
		return
	}

	select {
	case <-time.After(1 * time.Second):
		err = errTestFail
	case <-s.ch:
	}

	return
}
