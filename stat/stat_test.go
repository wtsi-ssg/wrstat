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
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/inconshreveable/log15"
	. "github.com/smartystreets/goconvey/convey"
)

func TestLstat(t *testing.T) {
	timeout := 50 * time.Millisecond
	attempts := 2
	consecutiveFails := 10

	Convey("Given a Statter with large timeout", t, func() {
		buff, l := newLogger()

		s := WithTimeout(timeout, attempts, consecutiveFails, l)
		So(s, ShouldNotBeNil)

		Convey("you can call Lstat on it", func() {
			info, err := s.Lstat("/foo/bar")
			So(err, ShouldNotBeNil)
			So(info, ShouldBeNil)

			pathEmpty, pathContent1, pathContent2 := createTestFiles(t)

			info, err = s.Lstat(pathEmpty)
			So(err, ShouldBeNil)
			So(info, ShouldNotBeNil)
			So(info.Size(), ShouldEqual, 0)

			info, err = s.Lstat(pathContent1)
			So(err, ShouldBeNil)
			So(info.Size(), ShouldEqual, 1)
			So(buff.String(), ShouldBeBlank)

			info, err = s.Lstat(pathContent2)
			So(err, ShouldBeNil)
			So(info.Size(), ShouldEqual, 1)
			So(buff.String(), ShouldBeBlank)

			Convey("but that fails with a tiny timeout", func() {
				s = WithTimeout(1*time.Nanosecond, attempts, consecutiveFails, l)
				So(s, ShouldNotBeNil)

				defer func() { os.Unsetenv("WRSTAT_TEST_LSTAT") }()
				os.Setenv("WRSTAT_TEST_LSTAT", "long")

				info, err = s.Lstat(pathContent1)
				So(err, ShouldNotBeNil)
				So(info, ShouldBeNil)

				logStr := buff.String()
				So(logStr, ShouldContainSubstring, `lvl=warn msg="an lstat call exceeded timeout, will retry"`)
				So(logStr, ShouldContainSubstring, `lvl=warn msg="an lstat call exceeded timeout, giving up"`)
				So(logStr, ShouldContainSubstring, `attempts=1`)
				So(logStr, ShouldContainSubstring, `attempts=2`)
				So(logStr, ShouldContainSubstring, `attempts=3`)
				So(logStr, ShouldNotContainSubstring, `attempts=4`)

				buff.Reset()

				info, err = s.Lstat("/foo")
				So(err, ShouldNotBeNil)
				So(info, ShouldBeNil)

				logStr = buff.String()
				So(logStr, ShouldContainSubstring, `lvl=warn msg="an lstat call exceeded timeout, will retry"`)
				So(logStr, ShouldContainSubstring, `lvl=warn msg="an lstat call exceeded timeout, giving up"`)
				So(logStr, ShouldContainSubstring, `attempts=1`)
				So(logStr, ShouldContainSubstring, `attempts=2`)
				So(logStr, ShouldContainSubstring, `attempts=3`)
				So(logStr, ShouldNotContainSubstring, `attempts=4`)

				Convey("after enough files fail consecutively it terminates", func() {
					s = WithTimeout(1*time.Nanosecond, attempts, 2, l)
					So(s, ShouldNotBeNil)

					defer func() { os.Unsetenv("WRSTAT_TEST_LSTAT") }()
					os.Setenv("WRSTAT_TEST_LSTAT", "long")

					info, err = s.Lstat(pathEmpty)
					So(err, ShouldEqual, errLstatSlow)
					So(info, ShouldBeNil)

					logStr = buff.String()
					So(logStr, ShouldNotContainSubstring, `too many lstat calls failed consecutively, terminating`)

					buff.Reset()

					info, err = s.Lstat(pathContent1)
					So(err, ShouldEqual, errLstatConsecFails)
					So(info, ShouldBeNil)

					logStr = buff.String()
					So(logStr, ShouldContainSubstring, `too many lstat calls failed consecutively, terminating`)

					So(s.failureCount, ShouldEqual, 2)
				})
			})

			Convey("which will correct invalid times", func() {
				s.defTime = time.Now().Add(-24 * time.Hour).Unix()

				err = os.Chtimes(pathContent1, time.Unix(0, 0), time.Unix(0, 0))
				So(err, ShouldBeNil)

				info, err = s.Lstat(pathContent1)
				So(err, ShouldBeNil)
				So(info, ShouldNotBeNil)

				stat, ok := info.Sys().(*syscall.Stat_t)
				So(ok, ShouldBeTrue)

				So(stat.Atim.Sec, ShouldEqual, s.defTime)
				So(stat.Mtim.Sec, ShouldEqual, 0)

				err = os.Chtimes(pathContent1, time.Now().Add(time.Hour), time.Now().Add(time.Hour*2))
				So(err, ShouldBeNil)

				info, err = s.Lstat(pathContent1)
				So(err, ShouldBeNil)
				So(info, ShouldNotBeNil)

				stat, ok = info.Sys().(*syscall.Stat_t)
				So(ok, ShouldBeTrue)

				So(stat.Atim.Sec, ShouldEqual, s.defTime)
				So(stat.Mtim.Sec, ShouldEqual, s.defTime)

				validTime := time.Now().Add(-48 * time.Hour)

				err = os.Chtimes(pathContent1, validTime, validTime)
				So(err, ShouldBeNil)

				info, err = s.Lstat(pathContent1)
				So(err, ShouldBeNil)
				So(info, ShouldNotBeNil)

				stat, ok = info.Sys().(*syscall.Stat_t)
				So(ok, ShouldBeTrue)

				So(stat.Atim.Sec, ShouldEqual, validTime.Unix())
				So(stat.Mtim.Sec, ShouldEqual, validTime.Unix())

				err = os.Chtimes(pathContent1, validTime, time.Unix(0, 0))
				So(err, ShouldBeNil)

				info, err = s.Lstat(pathContent1)
				So(err, ShouldBeNil)
				So(info, ShouldNotBeNil)

				stat, ok = info.Sys().(*syscall.Stat_t)
				So(ok, ShouldBeTrue)

				So(stat.Atim.Sec, ShouldEqual, validTime.Unix())
				So(stat.Mtim.Sec, ShouldEqual, 0)

				err = os.Chtimes(pathContent1, time.Unix(0, 0), validTime)
				So(err, ShouldBeNil)

				info, err = s.Lstat(pathContent1)
				So(err, ShouldBeNil)
				So(info, ShouldNotBeNil)

				stat, ok = info.Sys().(*syscall.Stat_t)
				So(ok, ShouldBeTrue)

				So(stat.Atim.Sec, ShouldEqual, validTime.Unix())
				So(stat.Mtim.Sec, ShouldEqual, validTime.Unix())
			})
		})
	})
}

// newLogger returns a logger that logs to the returned buffer.
func newLogger() (*bytes.Buffer, log15.Logger) { //nolint:ireturn
	buff := new(bytes.Buffer)
	l := log15.New()
	l.SetHandler(log15.StreamHandler(buff, log15.LogfmtFormat()))

	return buff, l
}

// createTestFiles creates 2 temp files, the first empty, the second 1 byte
// long, and returns their paths.
func createTestFiles(t *testing.T) (string, string, string) {
	t.Helper()

	dir := t.TempDir()
	pathEmpty := filepath.Join(dir, "empty")

	f, err := os.Create(pathEmpty)
	if err != nil {
		t.Fatal(err)
	}

	f.Close()

	pathContent1 := filepath.Join(dir, "content1")

	f, err = os.Create(pathContent1)
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.WriteString("1")
	if err != nil {
		t.Fatal(err)
	}

	f.Close()

	pathContent2 := filepath.Join(dir, "content2")

	f, err = os.Create(pathContent2)
	if err != nil {
		t.Fatal(err)
	}

	_, err = f.WriteString("2")
	if err != nil {
		t.Fatal(err)
	}

	f.Close()

	return pathEmpty, pathContent1, pathContent2
}
