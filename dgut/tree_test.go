/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
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

package dgut

import (
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/summary"
)

func TestTree(t *testing.T) {
	Convey("You can make a Tree from a dgut database", t, func() {
		paths := testMakeDBPaths(t)
		tree, errc := NewTree(paths[0])
		So(errc, ShouldNotBeNil)
		So(tree, ShouldBeNil)

		errc = testCreateDB(paths[0])
		So(errc, ShouldBeNil)

		tree, errc = NewTree(paths[0])
		So(errc, ShouldBeNil)
		So(tree, ShouldNotBeNil)

		Convey("You can query the Tree for DirInfo", func() {
			di, err := tree.DirInfo("/", nil)
			So(err, ShouldBeNil)
			So(di, ShouldResemble, &DirInfo{
				Current: &DirCountSize{"/", 14, 85},
				Children: []*DirCountSize{
					{"/a", 14, 85},
				},
			})

			di, err = tree.DirInfo("/a", nil)
			So(err, ShouldBeNil)
			So(di, ShouldResemble, &DirInfo{
				Current: &DirCountSize{"/a", 14, 85},
				Children: []*DirCountSize{
					{"/a/b", 9, 80},
					{"/a/c", 5, 5},
				},
			})

			di, err = tree.DirInfo("/a", &Filter{FTs: []summary.DirGUTFileType{1}})
			So(err, ShouldBeNil)
			So(di, ShouldResemble, &DirInfo{
				Current: &DirCountSize{"/a", 2, 10},
				Children: []*DirCountSize{
					{"/a/b", 2, 10},
				},
			})

			di, err = tree.DirInfo("/a/b/e/h/tmp", nil)
			So(err, ShouldBeNil)
			So(di, ShouldResemble, &DirInfo{
				Current:  &DirCountSize{"/a/b/e/h/tmp", 1, 5},
				Children: nil,
			})

			di, err = tree.DirInfo("/", &Filter{FTs: []summary.DirGUTFileType{3}})
			So(err, ShouldBeNil)
			So(di, ShouldResemble, &DirInfo{
				Current:  &DirCountSize{"/", 0, 0},
				Children: nil,
			})
		})

		Convey("You can find Where() in the Tree files are", func() {
			dcss, err := tree.Where("/", &Filter{GIDs: []uint32{1}, UIDs: []uint32{101}, FTs: []summary.DirGUTFileType{0}}, 0)
			So(err, ShouldBeNil)
			So(dcss, ShouldResemble, DCSs{
				{"/a/b/d", 3, 30},
			})

			dcss, err = tree.Where("/", &Filter{GIDs: []uint32{1}, UIDs: []uint32{101}}, 0)
			So(err, ShouldBeNil)
			So(dcss, ShouldResemble, DCSs{
				{"/a/b", 5, 40},
			})

			dcss, err = tree.Where("/", &Filter{GIDs: []uint32{1}, UIDs: []uint32{101}, FTs: []summary.DirGUTFileType{0}}, 1)
			So(err, ShouldBeNil)
			So(dcss, ShouldResemble, DCSs{
				{"/a/b/d", 3, 30},
				{"/a/b/d/g", 2, 20},
				{"/a/b/d/f", 1, 10},
			})

			dcss, err = tree.Where("/", &Filter{GIDs: []uint32{1}, UIDs: []uint32{101}, FTs: []summary.DirGUTFileType{0}}, 2)
			So(err, ShouldBeNil)
			So(dcss, ShouldResemble, DCSs{
				{"/a/b/d", 3, 30},
				{"/a/b/d/g", 2, 20},
				{"/a/b/d/f", 1, 10},
			})

			_, err = tree.Where("/foo", nil, 1)
			So(err, ShouldNotBeNil)
		})

		Convey("Queries fail with bad dirs", func() {
			_, err := tree.DirInfo("/foo", nil)
			So(err, ShouldNotBeNil)

			di := &DirInfo{Current: &DirCountSize{"/", 14, 85}}
			err = tree.addChildInfo(di, []string{"/foo"}, nil)
			So(err, ShouldNotBeNil)
		})

		Convey("You can't open the same db twice without closing it first", func() {
			_, err := NewTree(paths[0])
			So(err, ShouldNotBeNil)

			tree.Close()

			_, err = NewTree(paths[0])
			So(err, ShouldBeNil)
		})
	})

	Convey("You can make a Tree from multiple dgut databases and query it", t, func() {
		paths1 := testMakeDBPaths(t)
		db := NewDB(paths1[0])
		data := strings.NewReader("/\t1\t11\t1\t1\t1\n" +
			"/a\t1\t11\t1\t1\t1\n" +
			"/a/b\t1\t11\t1\t1\t1\n" +
			"/a/b/c\t1\t11\t1\t1\t1\n" +
			"/a/b/c/d\t1\t11\t1\t1\t1\n")
		err := db.Store(data, 20)
		So(err, ShouldBeNil)

		paths2 := testMakeDBPaths(t)
		db = NewDB(paths2[0])
		data = strings.NewReader("/\t1\t11\t1\t1\t1\n" +
			"/a\t1\t11\t1\t1\t1\n" +
			"/a/b\t1\t11\t1\t1\t1\n" +
			"/a/b/c\t1\t11\t1\t1\t1\n" +
			"/a/b/c/e\t1\t11\t1\t1\t1\n")
		err = db.Store(data, 20)
		So(err, ShouldBeNil)

		tree, err := NewTree(paths1[0], paths2[0])
		So(err, ShouldBeNil)
		So(tree, ShouldNotBeNil)

		dcss, err := tree.Where("/", nil, 0)
		So(err, ShouldBeNil)
		So(dcss, ShouldResemble, DCSs{
			{"/a/b/c", 2, 2},
		})

		dcss, err = tree.Where("/", nil, 1)
		So(err, ShouldBeNil)
		So(dcss, ShouldResemble, DCSs{
			{"/a/b/c", 2, 2},
			{"/a/b/c/d", 1, 1},
			{"/a/b/c/e", 1, 1},
		})
	})
}

func testCreateDB(path string) error {
	dgutData := testDGUTData()
	data := strings.NewReader(dgutData)
	db := NewDB(path)

	return db.Store(data, 20)
}