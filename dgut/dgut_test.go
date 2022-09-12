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
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/ugorji/go/codec"
	"github.com/wtsi-ssg/wrstat/summary"
	bolt "go.etcd.io/bbolt"
)

func TestDGUT(t *testing.T) {
	Convey("You can parse a single line of dgut data", t, func() {
		line := "/\t1\t101\t0\t3\t30\t50\n"
		dir, gut, err := parseDGUTLine(line)
		So(err, ShouldBeNil)
		So(dir, ShouldEqual, "/")
		So(gut, ShouldResemble, &GUT{GID: 1, UID: 101, FT: 0, Count: 3, Size: 30, Atime: 50})

		Convey("But invalid data won't parse", func() {
			_, _, err = parseDGUTLine("/\t1\t101\t0\t3\t50\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTLine("/\tfoo\t101\t0\t3\t30\t50\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTLine("/\t1\tfoo\t0\t3\t30\t50\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTLine("/\t1\t101\tfoo\t3\t30\t50\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTLine("/\t1\t101\t0\tfoo\t30\t50\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTLine("/\t1\t101\t0\t3\tfoo\t50\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTLine("/\t1\t101\t0\t3\t30\tfoo\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			So(err.Error(), ShouldEqual, "the provided data was not in dgut format")

			_, _, err = parseDGUTLine("\t\t\t\t\t\t\n")
			So(err, ShouldEqual, ErrBlankLine)

			So(err.Error(), ShouldEqual, "the provided line had no information")
		})
	})

	dgutData, expectedRootGUTs, expected, expectedKeys := testData()

	Convey("You can see if a GUT passes a filter", t, func() {
		filter := &Filter{}
		a, b := expectedRootGUTs[0].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)

		a, b = expectedRootGUTs[2].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeFalse)

		filter.GIDs = []uint32{3, 4, 5}
		a, b = expectedRootGUTs[0].PassesFilter(filter)
		So(a, ShouldBeFalse)
		So(b, ShouldBeFalse)

		filter.GIDs = []uint32{3, 2, 1}
		a, b = expectedRootGUTs[0].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)

		filter.UIDs = []uint32{103}
		a, b = expectedRootGUTs[0].PassesFilter(filter)
		So(a, ShouldBeFalse)
		So(b, ShouldBeFalse)

		filter.UIDs = []uint32{103, 102, 101}
		a, b = expectedRootGUTs[0].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)

		filter.FTs = []summary.DirGUTFileType{summary.DGUTFileTypeTemp}
		a, b = expectedRootGUTs[0].PassesFilter(filter)
		So(a, ShouldBeFalse)
		So(b, ShouldBeFalse)
		a, b = expectedRootGUTs[2].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)

		filter.FTs = []summary.DirGUTFileType{summary.DGUTFileTypeTemp, summary.DGUTFileTypeCram}
		a, b = expectedRootGUTs[0].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)
		a, b = expectedRootGUTs[2].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeFalse)

		filter.UIDs = nil
		a, b = expectedRootGUTs[0].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)

		filter.GIDs = nil
		a, b = expectedRootGUTs[0].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)
	})

	expectedUIDs := []uint32{101, 102}
	expectedGIDs := []uint32{1, 2}
	expectedFTs := []summary.DirGUTFileType{summary.DGUTFileTypeTemp, summary.DGUTFileTypeBam, summary.DGUTFileTypeCram}

	Convey("GUTs can sum the count and size and provide UIDs, GIDs and FTs of their GUT elements", t, func() {
		c, s, a, u, g, t := expectedRootGUTs.Summary(nil)
		So(c, ShouldEqual, 14)
		So(s, ShouldEqual, 85)
		So(a, ShouldEqual, 50)
		So(u, ShouldResemble, expectedUIDs)
		So(g, ShouldResemble, expectedGIDs)
		So(t, ShouldResemble, expectedFTs)
	})

	Convey("A DGUT can be encoded and decoded", t, func() {
		ch := new(codec.BincHandle)
		dirb, b := expected[0].encodeToBytes(ch)
		So(len(dirb), ShouldEqual, 1)
		So(len(b), ShouldEqual, 188)

		d := decodeDGUTbytes(ch, dirb, b)
		So(d, ShouldResemble, expected[0])
	})

	Convey("A DGUT can sum the count and size and provide UIDs, GIDs and FTs of its GUTs", t, func() {
		c, s, a, u, g, t := expected[0].Summary(nil)
		So(c, ShouldEqual, 14)
		So(s, ShouldEqual, 85)
		So(a, ShouldEqual, 50)
		So(u, ShouldResemble, expectedUIDs)
		So(g, ShouldResemble, expectedGIDs)
		So(t, ShouldResemble, expectedFTs)
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

		Convey("And database file paths", func() {
			paths := testMakeDBPaths(t)
			db := NewDB(paths[0])
			So(db, ShouldNotBeNil)

			Convey("You can store it in a database file", func() {
				_, errs := os.Stat(paths[1])
				So(errs, ShouldNotBeNil)
				_, errs = os.Stat(paths[2])
				So(errs, ShouldNotBeNil)

				err := db.Store(data, 4)
				So(err, ShouldBeNil)

				Convey("The resulting database files have the expected content", func() {
					info, errs := os.Stat(paths[1])
					So(errs, ShouldBeNil)
					So(info.Size(), ShouldBeGreaterThan, 10)
					info, errs = os.Stat(paths[2])
					So(errs, ShouldBeNil)
					So(info.Size(), ShouldBeGreaterThan, 10)

					keys, errt := testGetDBKeys(paths[1], gutBucket)
					So(errt, ShouldBeNil)
					So(keys, ShouldResemble, expectedKeys)

					keys, errt = testGetDBKeys(paths[2], childBucket)
					So(errt, ShouldBeNil)
					So(keys, ShouldResemble, []string{"/", "/a", "/a/b", "/a/b/d", "/a/b/e", "/a/b/e/h", "/a/c"})

					Convey("You can query a database after Open()ing it", func() {
						db = NewDB(paths[0])

						db.Close()

						err = db.Open()
						So(err, ShouldBeNil)

						c, s, a, u, g, t, errd := db.DirInfo("/", nil)
						So(errd, ShouldBeNil)
						So(c, ShouldEqual, 14)
						So(s, ShouldEqual, 85)
						So(a, ShouldEqual, 50)
						So(u, ShouldResemble, expectedUIDs)
						So(g, ShouldResemble, expectedGIDs)
						So(t, ShouldResemble, expectedFTs)

						c, s, a, u, g, t, errd = db.DirInfo("/a/c/d", nil)
						So(errd, ShouldBeNil)
						So(c, ShouldEqual, 5)
						So(s, ShouldEqual, 5)
						So(a, ShouldEqual, 90)
						So(u, ShouldResemble, []uint32{102})
						So(g, ShouldResemble, []uint32{2})
						So(t, ShouldResemble, []summary.DirGUTFileType{summary.DGUTFileTypeCram})

						c, s, a, u, g, t, errd = db.DirInfo("/a/b/d/g", nil)
						So(errd, ShouldBeNil)
						So(c, ShouldEqual, 6)
						So(s, ShouldEqual, 60)
						So(a, ShouldEqual, 60)
						So(u, ShouldResemble, expectedUIDs)
						So(g, ShouldResemble, []uint32{1})
						So(t, ShouldResemble, []summary.DirGUTFileType{summary.DGUTFileTypeCram})

						_, _, _, _, _, _, errd = db.DirInfo("/foo", nil)
						So(errd, ShouldNotBeNil)
						So(errd, ShouldEqual, ErrDirNotFound)

						c, s, a, u, g, t, errd = db.DirInfo("/", &Filter{GIDs: []uint32{1}})
						So(errd, ShouldBeNil)
						So(c, ShouldEqual, 9)
						So(s, ShouldEqual, 80)
						So(a, ShouldEqual, 50)
						So(u, ShouldResemble, expectedUIDs)
						So(g, ShouldResemble, []uint32{1})
						So(t, ShouldResemble, expectedFTs)

						c, s, a, u, g, t, errd = db.DirInfo("/", &Filter{UIDs: []uint32{102}})
						So(errd, ShouldBeNil)
						So(c, ShouldEqual, 9)
						So(s, ShouldEqual, 45)
						So(a, ShouldEqual, 75)
						So(u, ShouldResemble, []uint32{102})
						So(g, ShouldResemble, expectedGIDs)
						So(t, ShouldResemble, []summary.DirGUTFileType{summary.DGUTFileTypeCram})

						c, s, a, u, g, t, errd = db.DirInfo("/", &Filter{GIDs: []uint32{1}, UIDs: []uint32{102}})
						So(errd, ShouldBeNil)
						So(c, ShouldEqual, 4)
						So(s, ShouldEqual, 40)
						So(a, ShouldEqual, 75)
						So(u, ShouldResemble, []uint32{102})
						So(g, ShouldResemble, []uint32{1})
						So(t, ShouldResemble, []summary.DirGUTFileType{summary.DGUTFileTypeCram})

						c, s, a, u, g, t, errd = db.DirInfo("/", &Filter{
							GIDs: []uint32{1},
							UIDs: []uint32{102},
							FTs:  []summary.DirGUTFileType{summary.DGUTFileTypeTemp}})
						So(errd, ShouldBeNil)
						So(c, ShouldEqual, 0)
						So(s, ShouldEqual, 0)
						So(a, ShouldEqual, 0)
						So(u, ShouldResemble, []uint32{})
						So(g, ShouldResemble, []uint32{})
						So(t, ShouldResemble, []summary.DirGUTFileType{})

						c, s, a, u, g, t, errd = db.DirInfo("/", &Filter{FTs: []summary.DirGUTFileType{summary.DGUTFileTypeTemp}})
						So(errd, ShouldBeNil)
						So(c, ShouldEqual, 1)
						So(s, ShouldEqual, 5)
						So(a, ShouldEqual, 50)
						So(u, ShouldResemble, []uint32{101})
						So(g, ShouldResemble, []uint32{1})
						So(t, ShouldResemble, []summary.DirGUTFileType{summary.DGUTFileTypeTemp})

						children := db.Children("/a")
						So(children, ShouldResemble, []string{"/a/b", "/a/c"})

						children = db.Children("/a/b/e/h")
						So(children, ShouldResemble, []string{"/a/b/e/h/tmp"})

						children = db.Children("/a/c/d")
						So(children, ShouldBeNil)

						children = db.Children("/foo")
						So(children, ShouldBeNil)

						db.Close()
					})

					Convey("Open()s fail on invalid databases", func() {
						db = NewDB(paths[0])

						db.Close()

						err = os.RemoveAll(paths[2])
						So(err, ShouldBeNil)

						err = os.WriteFile(paths[2], []byte("foo"), 0600)
						So(err, ShouldBeNil)

						err = db.Open()
						So(err, ShouldNotBeNil)

						err = os.RemoveAll(paths[1])
						So(err, ShouldBeNil)

						err = os.WriteFile(paths[1], []byte("foo"), 0600)
						So(err, ShouldBeNil)

						err = db.Open()
						So(err, ShouldNotBeNil)
					})

					Convey("Store()ing multiple times", func() {
						data = strings.NewReader("/\t3\t103\t7\t2\t2\t25\n" +
							"/a/i\t3\t103\t7\t1\t1\t25\n" +
							"/i\t3\t103\t7\t1\t1\t30\n")

						Convey("to the same db file doesn't work", func() {
							err = db.Store(data, 4)
							So(err, ShouldNotBeNil)
							So(err, ShouldEqual, ErrDBExists)
						})

						Convey("to different db directories and loading them all does work", func() {
							path2 := paths[0] + ".2"
							err = os.Mkdir(path2, os.ModePerm)
							So(err, ShouldBeNil)
							db2 := NewDB(path2)
							err = db2.Store(data, 4)
							So(err, ShouldBeNil)

							db = NewDB(paths[0], path2)
							err = db.Open()
							So(err, ShouldBeNil)

							c, s, a, u, g, t, errd := db.DirInfo("/", nil)
							So(errd, ShouldBeNil)
							So(c, ShouldEqual, 16)
							So(s, ShouldEqual, 87)
							So(a, ShouldEqual, 25)
							So(u, ShouldResemble, []uint32{101, 102, 103})
							So(g, ShouldResemble, []uint32{1, 2, 3})
							So(t, ShouldResemble, expectedFTs)

							children := db.Children("/")
							So(children, ShouldResemble, []string{"/a", "/i"})

							children = db.Children("/a")
							So(children, ShouldResemble, []string{"/a/b", "/a/c", "/a/i"})
						})
					})
				})
			})

			Convey("Storing with a batch size == directories works", func() {
				err := db.Store(data, len(expectedKeys))
				So(err, ShouldBeNil)

				keys, errt := testGetDBKeys(paths[1], gutBucket)
				So(errt, ShouldBeNil)
				So(keys, ShouldResemble, expectedKeys)
			})

			Convey("Storing with a batch size > directories works", func() {
				err := db.Store(data, len(expectedKeys)+2)
				So(err, ShouldBeNil)

				keys, errt := testGetDBKeys(paths[1], gutBucket)
				So(errt, ShouldBeNil)
				So(keys, ShouldResemble, expectedKeys)
			})

			Convey("You can't store to db if data is invalid", func() {
				err := db.Store(strings.NewReader("foo"), 4)
				So(err, ShouldNotBeNil)
				So(db.writeErr, ShouldBeNil)
			})

			Convey("You can't store to db if", func() {
				db.batchSize = 4
				err := db.createDB()
				So(err, ShouldBeNil)

				Convey("the first db gets closed", func() {
					err = db.writeSet.dguts.Close()
					So(err, ShouldBeNil)

					db.writeErr = nil
					err = db.storeData(data)
					So(err, ShouldBeNil)
					So(db.writeErr, ShouldNotBeNil)
				})

				Convey("the second db gets closed", func() {
					err = db.writeSet.children.Close()
					So(err, ShouldBeNil)

					db.writeErr = nil
					err = db.storeData(data)
					So(err, ShouldBeNil)
					So(db.writeErr, ShouldNotBeNil)
				})

				Convey("the put fails", func() {
					db.writeBatch = expected

					err = db.writeSet.children.View(db.storeChildren)
					So(err, ShouldNotBeNil)

					err = db.writeSet.dguts.View(db.storeDGUTs)
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

			paths := testMakeDBPaths(t)
			db = NewDB(paths[0])

			err = os.WriteFile(paths[2], []byte("foo"), 0600)
			So(err, ShouldBeNil)

			err = db.Store(data, 4)
			So(err, ShouldNotBeNil)
		})
	})
}

// testData provides some test data and expected results.
func testData() (dgutData string, expectedRootGUTs GUTs, expected []*DGUT, expectedKeys []string) {
	dgutData = testDGUTData()

	expectedRootGUTs = GUTs{
		{GID: 1, UID: 101, FT: summary.DGUTFileTypeCram, Count: 3, Size: 30, Atime: 50},
		{GID: 1, UID: 101, FT: summary.DGUTFileTypeBam, Count: 2, Size: 10, Atime: 50},
		{GID: 1, UID: 101, FT: summary.DGUTFileTypeTemp, Count: 1, Size: 5, Atime: 50},
		{GID: 1, UID: 102, FT: summary.DGUTFileTypeCram, Count: 4, Size: 40, Atime: 75},
		{GID: 2, UID: 102, FT: summary.DGUTFileTypeCram, Count: 5, Size: 5, Atime: 75},
	}

	expected = []*DGUT{
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
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeCram, Count: 3, Size: 30, Atime: 50},
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeBam, Count: 2, Size: 10, Atime: 50},
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeTemp, Count: 1, Size: 5, Atime: 50},
				{GID: 1, UID: 102, FT: summary.DGUTFileTypeCram, Count: 4, Size: 40, Atime: 75},
			},
		},
		{
			Dir: "/a/b/d",
			GUTs: []*GUT{
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeCram, Count: 3, Size: 30, Atime: 50},
				{GID: 1, UID: 102, FT: summary.DGUTFileTypeCram, Count: 4, Size: 40, Atime: 75},
			},
		},
		{
			Dir: "/a/b/d/f",
			GUTs: []*GUT{
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeCram, Count: 1, Size: 10, Atime: 50},
			},
		},
		{
			Dir: "/a/b/d/g",
			GUTs: []*GUT{
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeCram, Count: 2, Size: 20, Atime: 60},
				{GID: 1, UID: 102, FT: summary.DGUTFileTypeCram, Count: 4, Size: 40, Atime: 75},
			},
		},
		{
			Dir: "/a/b/e",
			GUTs: []*GUT{
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeBam, Count: 2, Size: 10, Atime: 75},
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeTemp, Count: 1, Size: 5, Atime: 80},
			},
		},
		{
			Dir: "/a/b/e/h",
			GUTs: []*GUT{
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeBam, Count: 2, Size: 10, Atime: 75},
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeTemp, Count: 1, Size: 5, Atime: 80},
			},
		},
		{
			Dir: "/a/b/e/h/tmp",
			GUTs: []*GUT{
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeBam, Count: 1, Size: 5, Atime: 75},
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeTemp, Count: 1, Size: 5, Atime: 80},
			},
		},
		{
			Dir: "/a/c",
			GUTs: []*GUT{
				{GID: 2, UID: 102, FT: summary.DGUTFileTypeCram, Count: 5, Size: 5, Atime: 90},
			},
		},
		{
			Dir: "/a/c/d",
			GUTs: []*GUT{
				{GID: 2, UID: 102, FT: summary.DGUTFileTypeCram, Count: 5, Size: 5, Atime: 90},
			},
		},
	}

	expectedKeys = []string{"/", "/a", "/a/b", "/a/b/d", "/a/b/d/f",
		"/a/b/d/g", "/a/b/e", "/a/b/e/h", "/a/b/e/h/tmp", "/a/c", "/a/c/d"}

	return dgutData, expectedRootGUTs, expected, expectedKeys
}

func testDGUTData() string {
	return `/	1	101	7	3	30	50
/	1	101	6	2	10	50
/	1	101	1	1	5	50
/	1	102	7	4	40	75
/	2	102	7	5	5	75
/a	1	101	7	3	30	50
/a	1	101	6	2	10	50
/a	1	101	1	1	5	50
/a	1	102	7	4	40	75
/a	2	102	7	5	5	75
/a/b	1	101	7	3	30	50
/a/b	1	101	6	2	10	50
/a/b	1	101	1	1	5	50
/a/b	1	102	7	4	40	75
/a/b/d	1	101	7	3	30	50
/a/b/d	1	102	7	4	40	75
/a/b/d/f	1	101	7	1	10	50
/a/b/d/g	1	101	7	2	20	60
/a/b/d/g	1	102	7	4	40	75
/a/b/e	1	101	6	2	10	75
/a/b/e	1	101	1	1	5	80
/a/b/e/h	1	101	6	2	10	75
/a/b/e/h	1	101	1	1	5	80
/a/b/e/h/tmp	1	101	6	1	5	75
/a/b/e/h/tmp	1	101	1	1	5	80
/a/c	2	102	7	5	5	90
	2	102	7	5	5	100
/a/c/d	2	102	7	5	5	90
`
}

// testMakeDBPaths creates a temp dir that will be cleaned up automatically, and
// returns the paths to the directory and dgut and children database files
// inside that would be created. The files aren't actually created.
func testMakeDBPaths(t *testing.T) []string {
	t.Helper()

	dir := t.TempDir()

	set := newDBSet(dir)
	paths := set.paths()

	return append([]string{dir}, paths...)
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
