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
	"testing"
	"time"

	"github.com/inconshreveable/log15"
	. "github.com/smartystreets/goconvey/convey"
)

func TestLstat(t *testing.T) {
	timeout := 50 * time.Millisecond
	attempts := 2

	Convey("Given a Statter with large timeout", t, func() {
		buff := new(bytes.Buffer)
		l := log15.New()
		l.SetHandler(log15.StreamHandler(buff, log15.LogfmtFormat()))

		s := WithTimeout(timeout, attempts, l)
		So(s, ShouldNotBeNil)

		Convey("you can call Lstat on it", func() {
			dir := t.TempDir()
			pathEmpty := filepath.Join(dir, "empty")

			info, err := s.Lstat(pathEmpty)
			So(err, ShouldNotBeNil)
			So(info, ShouldBeNil)

			f, err := os.Create(pathEmpty)
			So(err, ShouldBeNil)
			f.Close()

			info, err = s.Lstat(pathEmpty)
			So(err, ShouldBeNil)
			So(info, ShouldNotBeNil)
			So(info.Size(), ShouldEqual, 0)

			pathContent := filepath.Join(dir, "content")
			f, err = os.Create(pathContent)
			So(err, ShouldBeNil)
			_, err = f.WriteString("1")
			So(err, ShouldBeNil)
			f.Close()

			info, err = s.Lstat(pathContent)
			So(err, ShouldBeNil)
			So(info.Size(), ShouldEqual, 1)
			So(buff.String(), ShouldBeBlank)

			Convey("but that fails with a tiny timeout", func() {
				s = WithTimeout(1*time.Nanosecond, attempts, l)
				So(s, ShouldNotBeNil)

				info, err = s.Lstat(pathContent)
				So(err, ShouldNotBeNil)
				So(info, ShouldBeNil)
				logStr := buff.String()
				So(logStr, ShouldContainSubstring, `lvl=warn msg="an lstat call exceeded timeout, will retry"`)
				So(logStr, ShouldContainSubstring, `lvl=warn msg="an lstat call exceeded timeout, giving up"`)
				So(logStr, ShouldContainSubstring, `attempts=1`)
				So(logStr, ShouldContainSubstring, `attempts=2`)
				So(logStr, ShouldContainSubstring, `attempts=3`)
				So(logStr, ShouldNotContainSubstring, `attempts=4`)
			})
		})
	})
}
