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
	bolt "go.etcd.io/bbolt"
)

func TestDGUT(t *testing.T) {
	Convey("You can parse a single line of dgut data", t, func() {
		line := "/\t1\t101\t0\t3\t30\n"
		dir, d, err := parseDGUTLine(line)
		So(err, ShouldBeNil)
		So(dir, ShouldEqual, "/")
		So(d, ShouldResemble, &GUT{GID: 1, UID: 101, FT: 0, Count: 3, Size: 30})

		Convey("But invalid data won't parse", func() {
			_, _, err = parseDGUTLine("/\t1\t101\t0\t3\n")
			So(err, ShouldEqual, errInvalidFormat)

			_, _, err = parseDGUTLine("/\tfoo\t101\t0\t3\t30\n")
			So(err, ShouldEqual, errInvalidFormat)

			_, _, err = parseDGUTLine("/\t1\tfoo\t0\t3\t30\n")
			So(err, ShouldEqual, errInvalidFormat)

			_, _, err = parseDGUTLine("/\t1\t101\tfoo\t3\t30\n")
			So(err, ShouldEqual, errInvalidFormat)

			_, _, err = parseDGUTLine("/\t1\t101\t0\tfoo\t30\n")
			So(err, ShouldEqual, errInvalidFormat)

			_, _, err = parseDGUTLine("/\t1\t101\t0\t3\tfoo\n")
			So(err, ShouldEqual, errInvalidFormat)

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

	Convey("A DGUT can be encoded and decoded", t, func() {
		dirb, b := expected[0].encodeToBytes()
		So(len(dirb), ShouldEqual, 1)
		So(len(b), ShouldEqual, 148)

		d, err := decodeDGUTbytes(dirb, b)
		So(err, ShouldBeNil)
		So(d, ShouldResemble, expected[0])
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

					keys, errt := testGetDBKeys(path)
					So(errt, ShouldBeNil)
					So(keys, ShouldResemble, expectedKeys)
				})
			})

			Convey("Storing with a batch size == directories works", func() {
				err := db.Store(data, len(expectedKeys))
				So(err, ShouldBeNil)

				keys, errt := testGetDBKeys(path)
				So(errt, ShouldBeNil)
				So(keys, ShouldResemble, expectedKeys)
			})

			Convey("Storing with a batch size > directories works", func() {
				err := db.Store(data, len(expectedKeys)+2)
				So(err, ShouldBeNil)

				keys, errt := testGetDBKeys(path)
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

				err = db.db.Close()
				So(err, ShouldBeNil)

				err = os.Remove(path)
				So(err, ShouldBeNil)

				err = db.storeData(data)
				So(err, ShouldBeNil)
				So(db.writeErr, ShouldNotBeNil)

				Convey("Or if the put fails", func() {
					err = db.createDB()
					So(err, ShouldBeNil)

					err = db.db.View(func(tx *bolt.Tx) error {
						b := tx.Bucket([]byte(gutBucket))

						return storeDGUTsInBucket(expected, b)
					})

					So(err, ShouldNotBeNil)
				})
			})
		})

		Convey("You can't store to an unwritable location", func() {
			db := NewDB("/dgut.db")
			So(db, ShouldNotBeNil)

			err := db.Store(data, 4)
			So(err, ShouldNotBeNil)
		})
	})
}

// testGetDBKeys returns all the keys in the gutBucket of the db at the given
// path.
func testGetDBKeys(path string) ([]string, error) {
	rdb, err := bolt.Open(path, 0666, &bolt.Options{ReadOnly: true})
	if err != nil {
		return nil, err
	}

	var keys []string

	err = rdb.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(gutBucket))

		return b.ForEach(func(k, v []byte) error {
			keys = append(keys, string(k))

			return nil
		})
	})

	return keys, err
}
