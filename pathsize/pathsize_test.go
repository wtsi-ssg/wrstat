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

package pathsize

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	bolt "go.etcd.io/bbolt"
)

func TestPathSize(t *testing.T) { //nolint:gocognit
	Convey("You can parse a single line of size data", t, func() {
		line := "/a/b/c/d.txt\t5\n"
		path, size, err := parsePathSizeLine(line)
		So(err, ShouldBeNil)
		So(path, ShouldEqual, "/a/b/c/d.txt")
		So(size, ShouldEqual, 5)

		Convey("But invalid data won't parse", func() {
			_, _, err = parsePathSizeLine("/a/b/c/d.txt\tfoo\n")
			So(err, ShouldNotBeNil)

			_, _, err = parsePathSizeLine("/a/b/c/d.txt\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			So(err.Error(), ShouldEqual, "the provided data was not in size format")

			_, _, err = parsePathSizeLine("\t\n")
			So(err, ShouldEqual, ErrBlankLine)
			So(err.Error(), ShouldEqual, "the provided line had no information")
		})
	})

	pathData, expected, expectedKeys := testData()

	Convey("Given multiline size data", t, func() {
		data := strings.NewReader(pathData)

		Convey("You can parse it", func() {
			i := 0
			cb := func(ps *PathSize) {
				So(ps, ShouldResemble, expected[i])
				i++
			}

			err := parsePathSizeLines(data, cb)
			So(err, ShouldBeNil)
			So(i, ShouldEqual, 5)
		})

		Convey("You can't parse invalid data", func() {
			data = strings.NewReader("foo")
			i := 0
			cb := func(ps *PathSize) {
				i++
			}

			err := parsePathSizeLines(data, cb)
			So(err, ShouldNotBeNil)
			So(i, ShouldEqual, 0)
		})

		Convey("And a database file path", func() {
			path := testMakeDBPath(t)
			db := NewDB(path)
			So(db, ShouldNotBeNil)

			Convey("You can store it in a database file", func() {
				_, errs := os.Stat(path)
				So(errs, ShouldNotBeNil)

				err := db.Store(data, 4)
				So(err, ShouldBeNil)

				Convey("The resulting database files have the expected content", func() {
					info, errs := os.Stat(path)
					So(errs, ShouldBeNil)
					So(info.Size(), ShouldBeGreaterThan, 10)

					keys, errt := testGetDBKeys(path, bucket)
					So(errt, ShouldBeNil)
					So(keys, ShouldResemble, expectedKeys)

					Convey("You can query a database after Open()ing it", func() {
						db = NewDB(path)

						db.Close()

						err = db.Open()
						So(err, ShouldBeNil)

						for _, ps := range expected {
							size, found := db.SizeOf(ps.Path)
							So(found, ShouldBeTrue)
							So(size, ShouldEqual, ps.Size)
						}

						size, found := db.SizeOf("/x/y.txt")
						So(found, ShouldBeFalse)
						So(size, ShouldEqual, 0)

						pss := db.GetChildren("/e")
						So(pss, ShouldResemble, []*PathSize{expected[1], expected[2], expected[3]})

						db.Close()
					})

					Convey("Open()s fail on invalid databases", func() {
						db = NewDB(path)

						db.Close()

						err = os.Remove(path)
						So(err, ShouldBeNil)

						err = os.WriteFile(path, []byte("foo"), 0600)
						So(err, ShouldBeNil)

						err = db.Open()
						So(err, ShouldNotBeNil)
					})

					Convey("Store()ing multiple times", func() {
						data = strings.NewReader("/x/y.txt\t99\n")

						Convey("to the same db file doesn't work", func() {
							err = db.Store(data, 4)
							So(err, ShouldNotBeNil)
							So(err, ShouldEqual, ErrDBExists)
						})

						Convey("to different db directories and loading them all does work", func() {
							path2 := path + ".2"
							db2 := NewDB(path2)
							err = db2.Store(data, 1)
							So(err, ShouldBeNil)

							db = NewDB(path, path2)
							err = db.Open()
							So(err, ShouldBeNil)

							for _, ps := range expected {
								size, found := db.SizeOf(ps.Path)
								So(found, ShouldBeTrue)
								So(size, ShouldEqual, ps.Size)
							}

							size, found := db.SizeOf("/x/y.txt")
							So(found, ShouldBeTrue)
							So(size, ShouldEqual, 99)
						})
					})
				})
			})

			Convey("You can't store to db if data is invalid", func() {
				err := db.Store(strings.NewReader("foo"), 4)
				So(err, ShouldNotBeNil)
				So(db.writeErr, ShouldBeNil)
			})
		})

		Convey("You can't Store to or Open an unwritable location", func() {
			db := NewDB("/dgut.db")
			So(db, ShouldNotBeNil)

			err := db.Store(data, 4)
			So(err, ShouldNotBeNil)

			err = db.Open()
			So(err, ShouldNotBeNil)

			path := testMakeDBPath(t)
			db = NewDB(path)

			err = os.WriteFile(path, []byte("foo"), 0600)
			So(err, ShouldBeNil)

			err = db.Store(data, 4)
			So(err, ShouldNotBeNil)
		})
	})
}

// testData provides some test data and expected results.
func testData() (string, []*PathSize, []string) {
	sizeData := testSizeData()

	expected := []*PathSize{
		{
			Path: "/a/b/c/d.txt",
			Size: 5,
		},
		{
			Path: "/e/f.txt",
			Size: 10,
		},
		{
			Path: "/e/g/h.txt",
			Size: 11,
		},
		{
			Path: "/e/i/j/k.txt",
			Size: 12,
		},
		{
			Path: "/l/m.txt",
			Size: 13,
		},
	}

	keys := []string{"/a/b/c/d.txt", "/e/f.txt", "/e/g/h.txt", "/e/i/j/k.txt", "/l/m.txt"}

	return sizeData, expected, keys
}

func testSizeData() string {
	return `/a/b/c/d.txt	5
/e/f.txt	10
/e/g/h.txt	11
/e/i/j/k.txt	12
/l/m.txt	13
`
}

// testMakeDBPath creats a temp dir and returns a path inside.
func testMakeDBPath(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()

	return filepath.Join(dir, "db")
}

// testGetDBKeys returns all the keys in the db at the given path.
func testGetDBKeys(path, bucket string) ([]string, error) {
	rdb, err := bolt.Open(path, dbOpenMode, nil)
	if err != nil {
		return nil, err
	}

	defer func() {
		err = rdb.Close()
	}()

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
