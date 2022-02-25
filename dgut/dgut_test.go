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
	"os"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/ugorji/go/codec"
	"github.com/wtsi-ssg/wrstat/summary"
	bolt "go.etcd.io/bbolt"
)

func TestDGUT(t *testing.T) {
	Convey("You can parse a single line of dgut data", t, func() {
		line := "/\t1\t101\t0\t3\t30\n"
		dir, gut, err := parseDGUTLine(line)
		So(err, ShouldBeNil)
		So(dir, ShouldEqual, "/")
		So(gut, ShouldResemble, &GUT{GID: 1, UID: 101, FT: 0, Count: 3, Size: 30})

		Convey("But invalid data won't parse", func() {
			_, _, err = parseDGUTLine("/\t1\t101\t0\t3\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTLine("/\tfoo\t101\t0\t3\t30\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTLine("/\t1\tfoo\t0\t3\t30\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTLine("/\t1\t101\tfoo\t3\t30\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTLine("/\t1\t101\t0\tfoo\t30\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTLine("/\t1\t101\t0\t3\tfoo\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			So(err.Error(), ShouldEqual, "the provided data was not in dgut format")
		})
	})

	dgutData := "/\t1\t101\t0\t3\t30\n" +
		"/\t1\t101\t1\t2\t10\n" +
		"/\t1\t101\t7\t1\t5\n" +
		"/\t1\t102\t0\t4\t40\n" +
		"/\t2\t102\t0\t5\t5\n" +

		"/a\t1\t101\t0\t3\t30\n" +
		"/a\t1\t101\t1\t2\t10\n" +
		"/a\t1\t101\t7\t1\t5\n" +
		"/a\t1\t102\t0\t4\t40\n" +
		"/a\t2\t102\t0\t5\t5\n" +

		"/a/b\t1\t101\t0\t3\t30\n" +
		"/a/b\t1\t101\t1\t2\t10\n" +
		"/a/b\t1\t101\t7\t1\t5\n" +
		"/a/b\t1\t102\t0\t4\t40\n" +

		"/a/b/d\t1\t101\t0\t3\t30\n" +
		"/a/b/d\t1\t102\t0\t4\t40\n" +

		"/a/b/d/f\t1\t101\t0\t1\t10\n" +
		"/a/b/d/g\t1\t101\t0\t2\t20\n" +
		"/a/b/d/g\t1\t102\t0\t4\t40\n" +

		"/a/b/e\t1\t101\t1\t2\t10\n" +
		"/a/b/e\t1\t101\t7\t1\t5\n" +
		"/a/b/e/h\t1\t101\t1\t2\t10\n" +
		"/a/b/e/h\t1\t101\t7\t1\t5\n" +
		"/a/b/e/h/tmp\t1\t101\t7\t1\t5\n" +

		"/a/c\t2\t102\t0\t5\t5\n" +
		"/a/c/d\t2\t102\t0\t5\t5\n"

	expectedRootGUTs := GUTs{
		{GID: 1, UID: 101, FT: 0, Count: 3, Size: 30},
		{GID: 1, UID: 101, FT: 1, Count: 2, Size: 10},
		{GID: 1, UID: 101, FT: 7, Count: 1, Size: 5},
		{GID: 1, UID: 102, FT: 0, Count: 4, Size: 40},
		{GID: 2, UID: 102, FT: 0, Count: 5, Size: 5},
	}
	expected := []*DGUT{
		{
			Dir:  "/",
			GUTs: expectedRootGUTs,
		},
		{
			Dir:  "/a",
			GUTs: expectedRootGUTs,
		},
		{
			Dir: "/a/b",
			GUTs: []*GUT{
				{GID: 1, UID: 101, FT: 0, Count: 3, Size: 30},
				{GID: 1, UID: 101, FT: 1, Count: 2, Size: 10},
				{GID: 1, UID: 101, FT: 7, Count: 1, Size: 5},
				{GID: 1, UID: 102, FT: 0, Count: 4, Size: 40},
			},
		},
		{
			Dir: "/a/b/d",
			GUTs: []*GUT{
				{GID: 1, UID: 101, FT: 0, Count: 3, Size: 30},
				{GID: 1, UID: 102, FT: 0, Count: 4, Size: 40},
			},
		},
		{
			Dir: "/a/b/d/f",
			GUTs: []*GUT{
				{GID: 1, UID: 101, FT: 0, Count: 1, Size: 10},
			},
		},
		{
			Dir: "/a/b/d/g",
			GUTs: []*GUT{
				{GID: 1, UID: 101, FT: 0, Count: 2, Size: 20},
				{GID: 1, UID: 102, FT: 0, Count: 4, Size: 40},
			},
		},
		{
			Dir: "/a/b/e",
			GUTs: []*GUT{
				{GID: 1, UID: 101, FT: 1, Count: 2, Size: 10},
				{GID: 1, UID: 101, FT: 7, Count: 1, Size: 5},
			},
		},
		{
			Dir: "/a/b/e/h",
			GUTs: []*GUT{
				{GID: 1, UID: 101, FT: 1, Count: 2, Size: 10},
				{GID: 1, UID: 101, FT: 7, Count: 1, Size: 5},
			},
		},
		{
			Dir: "/a/b/e/h/tmp",
			GUTs: []*GUT{
				{GID: 1, UID: 101, FT: 7, Count: 1, Size: 5},
			},
		},
		{
			Dir: "/a/c",
			GUTs: []*GUT{
				{GID: 2, UID: 102, FT: 0, Count: 5, Size: 5},
			},
		},
		{
			Dir: "/a/c/d",
			GUTs: []*GUT{
				{GID: 2, UID: 102, FT: 0, Count: 5, Size: 5},
			},
		},
	}

	expectedKeys := []string{"/", "/a", "/a/b", "/a/b/d", "/a/b/d/f",
		"/a/b/d/g", "/a/b/e", "/a/b/e/h", "/a/b/e/h/tmp", "/a/c", "/a/c/d"}

	Convey("You can see if a GUT passes a filter", t, func() {
		// expectedRootGUTs := GUTs{
		// 	{GID: 1, UID: 101, FT: 0, Count: 3, Size: 30},
		// 	{GID: 1, UID: 101, FT: 1, Count: 2, Size: 10},
		// 	{GID: 1, UID: 101, FT: 7, Count: 1, Size: 5},
		// 	{GID: 1, UID: 102, FT: 0, Count: 4, Size: 40},
		// 	{GID: 2, UID: 102, FT: 0, Count: 5, Size: 5},
		// }
		filter := &GUTFilter{}
		So(expectedRootGUTs[0].PassesFilter(filter), ShouldBeTrue)
		So(expectedRootGUTs[2].PassesFilter(filter), ShouldBeFalse)

		filter.GIDs = []uint32{3, 4, 5}
		So(expectedRootGUTs[0].PassesFilter(filter), ShouldBeFalse)

		filter.GIDs = []uint32{3, 2, 1}
		So(expectedRootGUTs[0].PassesFilter(filter), ShouldBeTrue)

		filter.UIDs = []uint32{103}
		So(expectedRootGUTs[0].PassesFilter(filter), ShouldBeFalse)

		filter.UIDs = []uint32{103, 102, 101}
		So(expectedRootGUTs[0].PassesFilter(filter), ShouldBeTrue)

		filter.FTs = []summary.DirGUTFileType{7}
		So(expectedRootGUTs[0].PassesFilter(filter), ShouldBeFalse)
		So(expectedRootGUTs[2].PassesFilter(filter), ShouldBeTrue)

		filter.FTs = []summary.DirGUTFileType{7, 0}
		So(expectedRootGUTs[0].PassesFilter(filter), ShouldBeTrue)
		So(expectedRootGUTs[2].PassesFilter(filter), ShouldBeFalse)

		filter.UIDs = nil
		So(expectedRootGUTs[0].PassesFilter(filter), ShouldBeTrue)

		filter.GIDs = nil
		So(expectedRootGUTs[0].PassesFilter(filter), ShouldBeTrue)
	})

	Convey("GUTs can sum the count and size of their GUT elements", t, func() {
		c, s := expectedRootGUTs.CountAndSize(nil)
		So(c, ShouldEqual, 14)
		So(s, ShouldEqual, 85)
	})

	Convey("A DGUT can be encoded and decoded", t, func() {
		dirb, b := expected[0].encodeToBytes(new(codec.BincHandle))
		So(len(dirb), ShouldEqual, 1)
		So(len(b), ShouldEqual, 148)

		d, err := decodeDGUTbytes(dirb, b)
		So(err, ShouldBeNil)
		So(d, ShouldResemble, expected[0])
	})

	Convey("A DGUT can sum the count and size of its GUTs", t, func() {
		c, s := expected[0].CountAndSize(nil)
		So(c, ShouldEqual, 14)
		So(s, ShouldEqual, 85)
	})

	Convey("Given multiline dgut data", t, func() {
		data := strings.NewReader(dgutData)

		Convey("You can parse it", func() {
			i := 0
			cb := func(dgut *DGUT) {
				So(dgut, ShouldResemble, expected[i])
				i++
			}

			err := parseDGUTLines(data, cb)
			So(err, ShouldBeNil)
			So(i, ShouldEqual, 11)
		})

		Convey("You can't parse invalid data", func() {
			data = strings.NewReader("foo")
			i := 0
			cb := func(dgut *DGUT) {
				i++
			}

			err := parseDGUTLines(data, cb)
			So(err, ShouldNotBeNil)
			So(i, ShouldEqual, 0)
		})

		Convey("And a database file path", func() {
			dir := t.TempDir()
			path := filepath.Join(dir, "dgut.db")
			db := NewDB(path)
			So(db, ShouldNotBeNil)

			Convey("You can store it in a database file", func() {
				info, errs := os.Stat(path)
				So(errs, ShouldNotBeNil)
				err := db.Store(data, 4)
				So(err, ShouldBeNil)

				Convey("The resulting database file has the expected content", func() {
					info, errs = os.Stat(path)
					So(errs, ShouldBeNil)
					So(info.Size(), ShouldBeGreaterThan, 30000)

					keys, errt := testGetDBKeys(path, gutBucket)
					So(errt, ShouldBeNil)
					So(keys, ShouldResemble, expectedKeys)

					keys, errt = testGetDBKeys(path, childBucket)
					So(errt, ShouldBeNil)
					So(keys, ShouldResemble, []string{"/", "/a", "/a/b", "/a/b/d", "/a/b/e", "/a/b/e/h", "/a/c"})

					Convey("You can query a database after Open()ing it", func() {
						db = NewDB(path)
						err = db.Open()
						So(err, ShouldBeNil)

						c, s, err := db.DirInfo("/", nil)
						So(err, ShouldBeNil)
						So(c, ShouldEqual, 14)
						So(s, ShouldEqual, 85)

						c, s, err = db.DirInfo("/a/c/d", nil)
						So(err, ShouldBeNil)
						So(c, ShouldEqual, 5)
						So(s, ShouldEqual, 5)

						c, s, err = db.DirInfo("/a/b/d/g", nil)
						So(err, ShouldBeNil)
						So(c, ShouldEqual, 6)
						So(s, ShouldEqual, 60)

						_, _, err = db.DirInfo("/foo", nil)
						So(err, ShouldNotBeNil)
						So(err, ShouldEqual, ErrDirNotFound)

						c, s, err = db.DirInfo("/", &GUTFilter{GIDs: []uint32{1}})
						So(err, ShouldBeNil)
						So(c, ShouldEqual, 9)
						So(s, ShouldEqual, 80)

						c, s, err = db.DirInfo("/", &GUTFilter{UIDs: []uint32{102}})
						So(err, ShouldBeNil)
						So(c, ShouldEqual, 9)
						So(s, ShouldEqual, 45)

						c, s, err = db.DirInfo("/", &GUTFilter{GIDs: []uint32{1}, UIDs: []uint32{102}})
						So(err, ShouldBeNil)
						So(c, ShouldEqual, 4)
						So(s, ShouldEqual, 40)

						c, s, err = db.DirInfo("/", &GUTFilter{
							GIDs: []uint32{1},
							UIDs: []uint32{102},
							FTs:  []summary.DirGUTFileType{summary.DGUTFileTypeTemp}})
						So(err, ShouldBeNil)
						So(c, ShouldEqual, 0)
						So(s, ShouldEqual, 0)

						c, s, err = db.DirInfo("/", &GUTFilter{FTs: []summary.DirGUTFileType{summary.DGUTFileTypeTemp}})
						So(err, ShouldBeNil)
						So(c, ShouldEqual, 1)
						So(s, ShouldEqual, 5)

						children, err := db.Children("/a")
						So(err, ShouldBeNil)
						So(children, ShouldResemble, []string{"/a/b", "/a/c"})

						children, err = db.Children("/a/b/e/h")
						So(err, ShouldBeNil)
						So(children, ShouldResemble, []string{"/a/b/e/h/tmp"})

						children, err = db.Children("/a/c/d")
						So(err, ShouldBeNil)
						So(children, ShouldResemble, []string{})

						children, err = db.Children("/foo")
						So(err, ShouldBeNil)
						So(children, ShouldResemble, []string{})
					})
				})
			})

			Convey("Storing with a batch size == directories works", func() {
				err := db.Store(data, len(expectedKeys))
				So(err, ShouldBeNil)

				keys, errt := testGetDBKeys(path, gutBucket)
				So(errt, ShouldBeNil)
				So(keys, ShouldResemble, expectedKeys)
			})

			Convey("Storing with a batch size > directories works", func() {
				err := db.Store(data, len(expectedKeys)+2)
				So(err, ShouldBeNil)

				keys, errt := testGetDBKeys(path, gutBucket)
				So(errt, ShouldBeNil)
				So(keys, ShouldResemble, expectedKeys)
			})

			Convey("You can't store to db if data is invalid", func() {
				err := db.Store(strings.NewReader("foo"), 4)
				So(err, ShouldNotBeNil)
				So(db.writeErr, ShouldBeNil)
			})

			Convey("You can't store to db if the db file gets deleted", func() {
				db.batchSize = 4
				err := db.createDB()
				So(err, ShouldBeNil)

				err = db.wdb.Close()
				So(err, ShouldBeNil)

				err = os.Remove(path)
				So(err, ShouldBeNil)

				err = db.storeData(data)
				So(err, ShouldBeNil)
				So(db.writeErr, ShouldNotBeNil)

				Convey("Or if the put fails", func() {
					err = db.createDB()
					So(err, ShouldBeNil)

					db.writeBatch = expected

					err = db.wdb.View(db.storeChildrenAndDGUTs)

					So(err, ShouldNotBeNil)
				})
			})
		})

		Convey("You can't Store to or Open an unwritable location", func() {
			db := NewDB("/dgut.db")
			So(db, ShouldNotBeNil)

			err := db.Store(data, 4)
			So(err, ShouldNotBeNil)

			err = db.Open()
			So(err, ShouldNotBeNil)
		})
	})
}

// testGetDBKeys returns all the keys in the given bucket of the db at the given
// path.
func testGetDBKeys(path, bucket string) ([]string, error) {
	rdb, err := openBoltReadOnly(path)
	if err != nil {
		return nil, err
	}

	var keys []string

	err = rdb.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucket))

		return b.ForEach(func(k, v []byte) error {
			keys = append(keys, string(k))

			return nil
		})
	})

	return keys, err
}
