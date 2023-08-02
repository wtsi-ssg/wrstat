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
	"io/fs"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestPerms(t *testing.T) {
	Convey("Given ch.tsv permission strings", t, func() {
		for n, test := range [...]struct {
			tsvPerms    string
			parsedPerms *ugoPerms
		}{
			{ // 1
				"*********",
				&ugoPerms{},
			},
			{ // 2
				"r********",
				&ugoPerms{
					user: perms{
						read: set,
					},
				},
			},
			{ // 3
				"*w*******",
				&ugoPerms{
					user: perms{
						write: set,
					},
				},
			},
			{ // 4
				"**x******",
				&ugoPerms{
					user: perms{
						execute: set,
					},
				},
			},
			{ // 5
				"***r*****",
				&ugoPerms{
					group: perms{
						read: set,
					},
				},
			},
			{ // 6
				"****w****",
				&ugoPerms{
					group: perms{
						write: set,
					},
				},
			},
			{ // 7
				"*****x***",
				&ugoPerms{
					group: perms{
						execute: set,
					},
				},
			},
			{ // 8
				"*****s***",
				&ugoPerms{
					group: perms{
						execute: set,
						sticky:  true,
					},
				},
			},
			{ // 9
				"******r**",
				&ugoPerms{
					other: perms{
						read: set,
					},
				},
			},
			{ // 10
				"*******w*",
				&ugoPerms{
					other: perms{
						write: set,
					},
				},
			},
			{ // 11
				"********x",
				&ugoPerms{
					other: perms{
						execute: set,
					},
				},
			},
			{ // 12
				"---------",
				&ugoPerms{
					user: perms{
						read:    unset,
						write:   unset,
						execute: unset,
					},
					group: perms{
						read:    unset,
						write:   unset,
						execute: unset,
					},
					other: perms{
						read:    unset,
						write:   unset,
						execute: unset,
					},
				},
			},
			{ // 13
				"^**^*****",
				&ugoPerms{
					user: perms{
						read: matchSet,
					},
					group: perms{
						read: matchSet,
					},
				},
			},
			{ // 14
				"*^*****^*",
				&ugoPerms{
					user: perms{
						write: matchSet,
					},
					other: perms{
						write: matchSet,
					},
				},
			},
			{ // 15
				"*****^**^",
				&ugoPerms{
					group: perms{
						execute: matchSet,
					},
					other: perms{
						execute: matchSet,
					},
				},
			},
		} {
			Convey("parseTSVPerms worked for test "+strconv.FormatInt(int64(n)+1, 10), func() {
				pp := parseTSVPerms(test.tsvPerms)
				So(pp, ShouldResemble, test.parsedPerms)
			})
		}
	})
}

func TestRules(t *testing.T) {
	Convey("Given a tsv reader", t, func() {
		reader := strings.NewReader(exampleTSVData)
		cr := NewTSVReader(reader)

		Convey("You can create a RulesStore", func() {
			rs := NewRulesStore()
			So(rs, ShouldNotBeNil)

			fakeUserToUID := func(userName string) (uint32, error) {
				return 1, nil
			}

			fakeGroupToGID := func(groupName string) (uint32, error) {
				switch groupName {
				case "group1":
					return 101, nil
				case "group2":
					return 102, nil
				}

				return 0, fs.ErrInvalid
			}

			fakeDirToUser := func(dir string) (uint32, error) {
				return 3, nil
			}

			fakeDirToGroup := func(dir string) (uint32, error) {
				return 103, nil
			}

			rs.userToUIDFunc = fakeUserToUID
			rs.groupToGIDFunc = fakeGroupToGID
			rs.dirToUserOwnerFunc = fakeDirToUser
			rs.dirToGroupOwnerFunc = fakeDirToGroup

			_, err := rs.FromTSV(cr)
			So(err, ShouldBeNil)

			Convey("And then retrieve rules for paths", func() {
				rule := rs.Get("/a/c/file.txt")
				So(rule, ShouldBeNil)

				rule = rs.Get("/a/b/c/d/sub/file.txt")
				So(rule, ShouldNotBeNil)

				So(rule.uid, ShouldEqual, 0)
				So(rule.changeUser, ShouldBeFalse)
				So(rule.gid, ShouldEqual, 0)
				So(rule.changeGroup, ShouldBeFalse)
				So(rule.filePerms, ShouldResemble, &ugoPerms{})
				So(rule.dirPerms, ShouldResemble, &ugoPerms{})

				So(rule.DesiredUser(1), ShouldEqual, 1)
				So(rule.DesiredUser(2), ShouldEqual, 2)
				So(rule.DesiredGroup(101), ShouldEqual, 101)
				So(rule.DesiredGroup(102), ShouldEqual, 102)
				So(rule.DesiredFilePerms(0777), ShouldEqual, 0777)
				So(rule.DesiredFilePerms(0123), ShouldEqual, 0123)
				So(rule.DesiredDirPerms(0777), ShouldEqual, 020000000777)
				So(rule.DesiredDirPerms(0123), ShouldEqual, 020000000123)

				rule = rs.Get("/e/f/g/sub/file.txt")
				So(rule, ShouldNotBeNil)

				So(rule.uid, ShouldEqual, 1)
				So(rule.changeUser, ShouldBeTrue)
				So(rule.gid, ShouldEqual, 101)
				So(rule.changeGroup, ShouldBeTrue)
				expectedUGO := &ugoPerms{
					user: perms{
						read:    set,
						write:   set,
						execute: set,
					},
					group: perms{
						read:    set,
						write:   set,
						execute: set,
					},
					other: perms{
						read:    set,
						write:   set,
						execute: set,
					},
				}
				So(rule.filePerms, ShouldResemble, expectedUGO)

				expectedUGO.group.sticky = true
				So(rule.dirPerms, ShouldResemble, expectedUGO)

				So(rule.DesiredUser(1), ShouldEqual, 1)
				So(rule.DesiredUser(2), ShouldEqual, 1)
				So(rule.DesiredGroup(101), ShouldEqual, 101)
				So(rule.DesiredGroup(102), ShouldEqual, 101)
				So(rule.DesiredFilePerms(0777), ShouldEqual, 0777)
				So(rule.DesiredFilePerms(0123), ShouldEqual, 0777)
				So(rule.DesiredDirPerms(0777), ShouldEqual, 020020000777)
				So(rule.DesiredDirPerms(0123), ShouldEqual, 020020000777)

				rule = rs.Get("/h/i/sub/file.txt")
				So(rule, ShouldNotBeNil)

				So(rule.uid, ShouldEqual, 0)
				So(rule.changeUser, ShouldBeFalse)
				So(rule.gid, ShouldEqual, 102)
				So(rule.changeGroup, ShouldBeTrue)

				So(rule.DesiredUser(1), ShouldEqual, 1)
				So(rule.DesiredUser(2), ShouldEqual, 2)
				So(rule.DesiredGroup(101), ShouldEqual, 102)
				So(rule.DesiredGroup(102), ShouldEqual, 102)
				So(rule.DesiredFilePerms(0000), ShouldEqual, 0000)
				So(rule.DesiredFilePerms(0777), ShouldEqual, 0000)
				So(rule.DesiredDirPerms(0000), ShouldEqual, 020000000000)
				So(rule.DesiredDirPerms(0777), ShouldEqual, 020000000000)

				rule = rs.Get("/a/b/c/	d/sub/file.txt")
				So(rule, ShouldNotBeNil)

				So(rule.uid, ShouldEqual, 3)
				So(rule.changeUser, ShouldBeTrue)
				So(rule.gid, ShouldEqual, 103)
				So(rule.changeGroup, ShouldBeTrue)

				So(rule.DesiredUser(3), ShouldEqual, 3)
				So(rule.DesiredUser(2), ShouldEqual, 3)
				So(rule.DesiredGroup(103), ShouldEqual, 103)
				So(rule.DesiredGroup(102), ShouldEqual, 103)
				So(rule.DesiredFilePerms(0000), ShouldEqual, 0000)
				So(rule.DesiredFilePerms(0700), ShouldEqual, 0777)
				So(rule.DesiredFilePerms(0060), ShouldEqual, 0666)
				So(rule.DesiredFilePerms(0005), ShouldEqual, 0555)
				So(rule.DesiredDirPerms(0000), ShouldEqual, 020000000000)
				So(rule.DesiredDirPerms(0700), ShouldEqual, 020000000777)
				So(rule.DesiredDirPerms(0060), ShouldEqual, 020000000666)
				So(rule.DesiredDirPerms(0005), ShouldEqual, 020000000555)
			})
		})
	})
}

func TestRulesWithRealData(t *testing.T) {
	Convey("Given some real files, can create a TSV", t, func() {
		tc := newTSVCreator(t)
		tc.add("some/prefix", "^", "^")

		u, err := user.Current()
		So(err, ShouldBeNil)

		uid, err := strconv.ParseUint(u.Uid, 10, 32)
		So(err, ShouldBeNil)

		g, err := user.LookupGroupId(u.Gid)
		So(err, ShouldBeNil)

		gid, err := strconv.ParseUint(g.Gid, 10, 32)
		So(err, ShouldBeNil)

		tc.add("some/other/prefix", u.Username, g.Name)

		cr := tc.asTSVReader()
		So(cr, ShouldNotBeNil)

		Convey("You can create a RulesStore", func() {
			rs, err := NewRulesStore().FromTSV(cr)
			So(err, ShouldBeNil)
			So(rs, ShouldNotBeNil)

			Convey("And then retrieve rules for paths", func() {
				rule := rs.Get(filepath.Join(tc.Paths[0], "sub", "file.txt"))
				So(rule, ShouldNotBeNil)

				So(rule.uid, ShouldEqual, uid)
				So(rule.changeUser, ShouldBeTrue)
				So(rule.gid, ShouldEqual, gid)
				So(rule.changeGroup, ShouldBeTrue)

				rule = rs.Get(filepath.Join(tc.Paths[1], "sub", "file.txt"))
				So(rule, ShouldNotBeNil)

				So(rule.uid, ShouldEqual, uid)
				So(rule.changeUser, ShouldBeTrue)
				So(rule.gid, ShouldEqual, gid)
				So(rule.changeGroup, ShouldBeTrue)
			})
		})
	})
}

type tsvCreator struct {
	prefix string
	strings.Builder
	Paths []string
}

func newTSVCreator(t *testing.T) *tsvCreator {
	t.Helper()

	return &tsvCreator{
		prefix: t.TempDir(),
	}
}

func (t *tsvCreator) add(path, user, group string) {
	path = filepath.Join(t.prefix, path)

	t.Paths = append(t.Paths, path)

	err := os.MkdirAll(path, 0755)
	So(err, ShouldBeNil)

	t.Builder.WriteString(path + "	" + user + "	" + group + "	*********	*********\n")
}

func (t *tsvCreator) asTSVReader() *TSVReader {
	return NewTSVReader(strings.NewReader(t.String()))
}
