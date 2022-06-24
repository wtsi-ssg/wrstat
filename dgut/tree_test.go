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

		expectedUIDs := []uint32{101, 102}
		expectedGIDs := []uint32{1, 2}
		expectedFTs := []summary.DirGUTFileType{0, 1}
		expectedUIDsOne := []uint32{101}
		expectedGIDsOne := []uint32{1}
		expectedFTsZero := []summary.DirGUTFileType{0}
		expectedFTsOne := []summary.DirGUTFileType{1}

		Convey("You can query the Tree for DirInfo", func() {
			di, err := tree.DirInfo("/", nil)
			So(err, ShouldBeNil)
			So(di, ShouldResemble, &DirInfo{
				Current: &DirSummary{"/", 14, 85, expectedUIDs, expectedGIDs, expectedFTs},
				Children: []*DirSummary{
					{"/a", 14, 85, expectedUIDs, expectedGIDs, expectedFTs},
				},
			})

			di, err = tree.DirInfo("/a", nil)
			So(err, ShouldBeNil)
			So(di, ShouldResemble, &DirInfo{
				Current: &DirSummary{"/a", 14, 85, expectedUIDs, expectedGIDs, expectedFTs},
				Children: []*DirSummary{
					{"/a/b", 9, 80, expectedUIDs, expectedGIDsOne, expectedFTs},
					{"/a/c", 5, 5, []uint32{102}, []uint32{2}, expectedFTsZero},
				},
			})

			di, err = tree.DirInfo("/a", &Filter{FTs: []summary.DirGUTFileType{1}})
			So(err, ShouldBeNil)
			So(di, ShouldResemble, &DirInfo{
				Current: &DirSummary{"/a", 2, 10, expectedUIDsOne, expectedGIDsOne, expectedFTsOne},
				Children: []*DirSummary{
					{"/a/b", 2, 10, expectedUIDsOne, expectedGIDsOne, expectedFTsOne},
				},
			})

			di, err = tree.DirInfo("/a/b/e/h/tmp", nil)
			So(err, ShouldBeNil)
			So(di, ShouldResemble, &DirInfo{
				Current:  &DirSummary{"/a/b/e/h/tmp", 1, 5, expectedUIDsOne, expectedGIDsOne, expectedFTsOne},
				Children: nil,
			})

			di, err = tree.DirInfo("/", &Filter{FTs: []summary.DirGUTFileType{3}})
			So(err, ShouldBeNil)
			So(di, ShouldResemble, &DirInfo{
				Current:  &DirSummary{"/", 0, 0, []uint32{}, []uint32{}, []summary.DirGUTFileType{}},
				Children: nil,
			})
		})

		Convey("You can ask the Tree if a dir has children", func() {
			has := tree.DirHasChildren("/", nil)
			So(has, ShouldBeTrue)

			has = tree.DirHasChildren("/a/b/e/h/tmp", nil)
			So(has, ShouldBeFalse)

			has = tree.DirHasChildren("/", &Filter{
				GIDs: []uint32{9999},
			})
			So(has, ShouldBeFalse)

			has = tree.DirHasChildren("/foo", nil)
			So(has, ShouldBeFalse)
		})

		Convey("You can find Where() in the Tree files are", func() {
			dcss, err := tree.Where("/", &Filter{GIDs: []uint32{1}, UIDs: []uint32{101}, FTs: []summary.DirGUTFileType{0}}, 0)
			So(err, ShouldBeNil)
			So(dcss, ShouldResemble, DCSs{
				{"/a/b/d", 3, 30, expectedUIDsOne, expectedGIDsOne, expectedFTsZero},
			})

			dcss, err = tree.Where("/", &Filter{GIDs: []uint32{1}, UIDs: []uint32{101}}, 0)
			So(err, ShouldBeNil)
			So(dcss, ShouldResemble, DCSs{
				{"/a/b", 5, 40, expectedUIDsOne, expectedGIDsOne, expectedFTs},
			})

			dcss, err = tree.Where("/", &Filter{GIDs: []uint32{1}, UIDs: []uint32{101}, FTs: []summary.DirGUTFileType{0}}, 1)
			So(err, ShouldBeNil)
			So(dcss, ShouldResemble, DCSs{
				{"/a/b/d", 3, 30, expectedUIDsOne, expectedGIDsOne, expectedFTsZero},
				{"/a/b/d/g", 2, 20, expectedUIDsOne, expectedGIDsOne, expectedFTsZero},
				{"/a/b/d/f", 1, 10, expectedUIDsOne, expectedGIDsOne, expectedFTsZero},
			})

			dcss.SortByDir()
			So(dcss, ShouldResemble, DCSs{
				{"/a/b/d", 3, 30, expectedUIDsOne, expectedGIDsOne, expectedFTsZero},
				{"/a/b/d/f", 1, 10, expectedUIDsOne, expectedGIDsOne, expectedFTsZero},
				{"/a/b/d/g", 2, 20, expectedUIDsOne, expectedGIDsOne, expectedFTsZero},
			})

			dcss, err = tree.Where("/", &Filter{GIDs: []uint32{1}, UIDs: []uint32{101}, FTs: []summary.DirGUTFileType{0}}, 2)
			So(err, ShouldBeNil)
			So(dcss, ShouldResemble, DCSs{
				{"/a/b/d", 3, 30, expectedUIDsOne, expectedGIDsOne, expectedFTsZero},
				{"/a/b/d/g", 2, 20, expectedUIDsOne, expectedGIDsOne, expectedFTsZero},
				{"/a/b/d/f", 1, 10, expectedUIDsOne, expectedGIDsOne, expectedFTsZero},
			})

			_, err = tree.Where("/foo", nil, 1)
			So(err, ShouldNotBeNil)
		})

		Convey("You can get the FileLocations()", func() {
			dcss, err := tree.FileLocations("/",
				&Filter{GIDs: []uint32{1}, UIDs: []uint32{101}, FTs: []summary.DirGUTFileType{0}})
			So(err, ShouldBeNil)

			So(dcss, ShouldResemble, DCSs{
				{"/a/b/d/f", 1, 10, expectedUIDsOne, expectedGIDsOne, expectedFTsZero},
				{"/a/b/d/g", 2, 20, expectedUIDsOne, expectedGIDsOne, expectedFTsZero},
			})

			_, err = tree.FileLocations("/foo", nil)
			So(err, ShouldNotBeNil)
		})

		Convey("Queries fail with bad dirs", func() {
			_, err := tree.DirInfo("/foo", nil)
			So(err, ShouldNotBeNil)

			di := &DirInfo{Current: &DirSummary{"/", 14, 85, expectedUIDs, expectedGIDs, expectedFTs}}
			err = tree.addChildInfo(di, []string{"/foo"}, nil)
			So(err, ShouldNotBeNil)
		})

		Convey("Closing works", func() {
			tree.Close()
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
			{"/a/b/c", 2, 2, []uint32{11}, []uint32{1}, []summary.DirGUTFileType{1}},
		})

		dcss, err = tree.Where("/", nil, 1)
		So(err, ShouldBeNil)
		So(dcss, ShouldResemble, DCSs{
			{"/a/b/c", 2, 2, []uint32{11}, []uint32{1}, []summary.DirGUTFileType{1}},
			{"/a/b/c/d", 1, 1, []uint32{11}, []uint32{1}, []summary.DirGUTFileType{1}},
			{"/a/b/c/e", 1, 1, []uint32{11}, []uint32{1}, []summary.DirGUTFileType{1}},
		})
	})
}

func testCreateDB(path string) error {
	dgutData := testDGUTData()
	data := strings.NewReader(dgutData)
	db := NewDB(path)

	return db.Store(data, 20)
}
