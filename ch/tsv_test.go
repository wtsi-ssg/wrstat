/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
 *          Sendu Bala <sb10@sanger.ac.uk>
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

package ch

import (
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const exampleTSVData = `/a/b/c/d	*	*	*********	*********
/e/f/g	user1	group1	rwxrwxrwx	rwxrwsrwx
/h/i	*	group2	---------	---------

# humgen
/a/b/c/	d	^	^	^^^^^^^^^	^^^^^^^^^
`

// directory user group fileperms dirperms
//
// *perms format is rwxrwxrwx for user,group,other, where - means remove the
// permission, * means leave it unchanged, and a letter means set it. s for the
// group x would enable setting group sticky bit. s implies x. Could use ^ in at
// least 2 equivalent places to mean "set all if any set". ie. '**^**^***` would
// mean "change nothing, except if execute is set on user or group, set it on
// both".
//
// user and group can be unix username or unix group name. * means don't set it.
// Could use ^ to mean copy from the directory.

func TestTSV(t *testing.T) {
	Convey("Given a prefix tsv file", t, func() {
		reader := strings.NewReader(exampleTSVData)

		Convey("You can create a ch.tsv reader for it and read each row", func() {
			cr := NewTSVReader(reader)
			rules := 0

			for cr.Next() {
				cols := cr.Columns()

				So(len(cols), ShouldEqual, 5)
				So(cols[0], ShouldStartWith, "/")
				So(cols[4], ShouldNotEndWith, "\n")

				rules++
			}

			So(cr.Error(), ShouldBeNil)
			So(rules, ShouldEqual, 4)
		})
	})

	Convey("Given bad tsv files, the reader returns errors", t, func() {
		data := []string{
			"/a/b/c/d	*	*	*********	*********",
			"/a/b/c/d	*	*	*********	\n",
			"/a/b/c/d	*	*	*********	*********	x\n",
			"/a/b/c/d	*	*	*********	********\n",
			"/a/b/c/d	*	*	********	*********\n",
			"/a/b/c/d	%	*	*********	*********\n",
			"/a/b/c/d	*	%	*********	*********\n",
			"/a/b/c/d	*	*	*********	^********\n",
			"/a/b/c/d	*	*	*********	****^****\n",
			"/a/b/c/d	*	*	*********	********^\n",
		}

		for _, line := range data {
			reader := strings.NewReader(line)
			cr := NewTSVReader(reader)

			So(cr.Next(), ShouldEqual, false)
			So(cr.Error(), ShouldNotBeNil)
		}
	})
}
