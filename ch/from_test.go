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

package ch

import (
	"bytes"
	"fmt"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestGIDFromSubDir(t *testing.T) {
	primaryGID, otherGIDs := getGIDs(t)

	if len(otherGIDs) == 0 {
		SkipConvey("Can't test GIDFromSubDir since you don't belong to multiple groups")

		return
	}

	primaryName := testGroupName(t, primaryGID)
	otherName := testGroupName(t, otherGIDs[0])

	Convey("Given a GIDFromSubDir's PathChecker", t, func() {
		buff, l := newLogger()

		f, err := NewGIDFromSubDir(
			[]string{"/disk1", "/disk2/sub", "/disk3"},
			"teams",
			map[string]string{"foo": otherName},
			"projects",
			map[string]int{primaryName: otherGIDs[0]},
			l,
		)
		So(err, ShouldBeNil)
		So(f, ShouldNotBeNil)

		p := f.PathChecker()
		So(p, ShouldNotBeNil)

		testPaths(p, otherGIDs[0], primaryName, otherName, buff)
	})

	Convey("NewGIDFromSubDir fails with a bad lookup", t, func() {
		_, l := newLogger()

		f, err := NewGIDFromSubDir(
			[]string{"/disk1", "/disk2/sub", "/disk3"},
			"teams",
			map[string]string{"foo": "!@£$"},
			"projects",
			map[string]int{primaryName: otherGIDs[0]},
			l,
		)
		So(err, ShouldNotBeNil)
		So(f, ShouldBeNil)
	})

	Convey("You can create a GIDFromSubDir from YAML", t, func() {
		buff, l := newLogger()

		data := `
prefixes: ["/disk1", "/disk2/sub", "/disk3"]
lookupDir: teams
lookup:
  foo: ` + otherName + `
directDir: projects
exceptions:
  ` + fmt.Sprintf("%s: %d\n", primaryName, otherGIDs[0])

		f, err := NewGIDFromSubDirFromYAML([]byte(data), l)
		So(err, ShouldBeNil)
		So(f, ShouldNotBeNil)

		p := f.PathChecker()
		So(p, ShouldNotBeNil)

		testPaths(p, otherGIDs[0], primaryName, otherName, buff)
	})

	Convey("You can't create a GIDFromSubDir from invalid YAML", t, func() {
		_, l := newLogger()

		data := `
prefixes: ["/disk1", "/disk2/sub", "/disk3"}
lookupDir: teams
`

		f, err := NewGIDFromSubDirFromYAML([]byte(data), l)
		So(err, ShouldNotBeNil)
		So(f, ShouldBeNil)

		data = `prefixes: ["/disk1", "/disk2/sub", "/disk3"]`

		f, err = NewGIDFromSubDirFromYAML([]byte(data), l)
		So(err, ShouldNotBeNil)
		So(err, ShouldEqual, errInvalidYAML)
		So(f, ShouldBeNil)
	})
}

// testPaths tests that the PathChecker behaves as expected.
func testPaths(p PathChecker, expectedGID int, expectedName1, expectedName2 string, buff *bytes.Buffer) {
	Convey("Valid paths return GIDs", func() {
		ok, gid := p("/disk1/teams/foo/file1.txt")
		So(ok, ShouldBeTrue)
		So(gid, ShouldEqual, expectedGID)

		ok, gid = p("/disk1/projects/" + expectedName1 + "/file2.txt")
		So(ok, ShouldBeTrue)
		So(gid, ShouldEqual, expectedGID)

		ok, gid = p("/disk1/projects/" + expectedName2 + "/file2.txt")
		So(ok, ShouldBeTrue)
		So(gid, ShouldEqual, expectedGID)
	})

	Convey("Invalid paths return false and log errors", func() {
		ok, gid := p("/disk3/file4.txt")
		So(ok, ShouldBeFalse)
		So(gid, ShouldEqual, 0)
		So(buff.String(), ShouldBeBlank)

		ok, gid = p("/disk1/teams/bar/file1.txt")
		So(ok, ShouldBeFalse)
		So(gid, ShouldEqual, badUnixGroup)
		So(buff.String(), ShouldContainSubstring, "subdir not in group lookup")
		buff.Reset()

		ok, gid = p("/disk1/projects/bar/file2.txt")
		So(ok, ShouldBeFalse)
		So(gid, ShouldEqual, badUnixGroup)
		So(buff.String(), ShouldContainSubstring, "subdir not a unix group name")
	})
}
