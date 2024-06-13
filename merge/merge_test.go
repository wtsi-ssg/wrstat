/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
 *
 * Author: Daniel Elia <de7@sanger.ac.uk>
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

package merge

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestSomething(t *testing.T) {
	Convey("Given a non-existent directory, an error should be returned.", t, func() {
		dir1 := "hello"
		dir2 := "world"
		err := MergeDB(dir1, dir2, false)
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, errDirectoryInvalid.Error())
	})
	Convey("Given a valid empty directory", t, func() {
		dir1 := t.TempDir()
		dir2 := t.TempDir()
		err := MergeDB(dir1, dir2, false)
		So(err, ShouldBeNil)
	})
	Convey("Given a valid directory with incorrectly named files", t, func() {})
	Convey("Given a valid directory with empty files", t, func() {})
	Convey("Given a valid directory with small files", t, func() {})
}
