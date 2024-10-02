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
	"math"
	"os"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/ugorji/go/codec"
	internaldata "github.com/wtsi-ssg/wrstat/v5/internal/data"
	"github.com/wtsi-ssg/wrstat/v5/internal/encode"
	"github.com/wtsi-ssg/wrstat/v5/summary"
	bolt "go.etcd.io/bbolt"
)

func TestDGUT(t *testing.T) {
	Convey("You can parse a single line of dgut data", t, func() {
		line := encode.Base64Encode("/") + "\t1\t101\t0\t3\t30\t50\t50\t0\t1\t1\t2\t3\t3\t3\t5\t0\t0\t0\t1\t2\t3\t3\t5\n"
		dir, gut, err := parseDGUTLine(line)
		So(err, ShouldBeNil)
		So(dir, ShouldEqual, "/")
		So(gut, ShouldResemble, &GUT{GID: 1, UID: 101, FT: 0, Count: 3, Size: 30, Atime: 50, Mtime: 50,
			SizeByAccessAge: [8]int64{0, 1, 1, 2, 3, 3, 3, 5},
			SizeByModifyAge: [8]int64{0, 0, 0, 1, 2, 3, 3, 5}})

		Convey("But invalid data won't parse", func() {
			_, _, err = parseDGUTLine(encode.Base64Encode("/") +
				"\t1\t101\t0\t3\t50\t50\t0\t1\t1\t2\t3\t3\t3\t5\t0\t0\t0\t1\t2\t3\t3\t5\n")

			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTLine(encode.Base64Encode("/") +
				"\tfoo\t101\t0\t3\t30\t50\t50\t0\t1\t1\t2\t3\t3\t3\t5\t0\t0\t0\t1\t2\t3\t3\t5\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTLine(encode.Base64Encode("/") +
				"\t1\tfoo\t0\t3\t30\t50\t50\t0\t1\t1\t2\t3\t3\t3\t5\t0\t0\t0\t1\t2\t3\t3\t5\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTLine(encode.Base64Encode("/") +
				"\t1\t101\tfoo\t3\t30\t50\t50\t0\t1\t1\t2\t3\t3\t3\t5\t0\t0\t0\t1\t2\t3\t3\t5\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTLine(encode.Base64Encode("/") +
				"\t1\t101\t0\tfoo\t30\t50\t50\t0\t1\t1\t2\t3\t3\t3\t5\t0\t0\t0\t1\t2\t3\t3\t5\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTLine(encode.Base64Encode("/") +
				"\t1\t101\t0\t3\tfoo\t50\t50\t0\t1\t1\t2\t3\t3\t3\t5\t0\t0\t0\t1\t2\t3\t3\t5\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTLine(encode.Base64Encode("/") +
				"\t1\t101\t0\t3\t30\tfoo\t50\t0\t1\t1\t2\t3\t3\t3\t5\t0\t0\t0\t1\t2\t3\t3\t5\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTLine(encode.Base64Encode("/") +
				"\t1\t101\t0\t3\t30\t50\tfoo\t0\t1\t1\t2\t3\t3\t3\t5\t0\t0\t0\t1\t2\t3\t3\t5\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			So(err.Error(), ShouldEqual, "the provided data was not in dgut format")

			_, _, err = parseDGUTLine("\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\t\n")
			So(err, ShouldEqual, ErrBlankLine)

			So(err.Error(), ShouldEqual, "the provided line had no information")
		})
	})

	dgutData, expectedRootGUTs, expected, expectedKeys := testData(t)

	Convey("You can see if a GUT passes a filter", t, func() {
		filter := &Filter{}
		a, b := expectedRootGUTs[3].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)

		a, b = expectedRootGUTs[0].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeFalse)

		filter.GIDs = []uint32{3, 4, 5}
		a, b = expectedRootGUTs[3].PassesFilter(filter)
		So(a, ShouldBeFalse)
		So(b, ShouldBeFalse)

		filter.GIDs = []uint32{3, 2, 1}
		a, b = expectedRootGUTs[3].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)

		filter.UIDs = []uint32{103}
		a, b = expectedRootGUTs[3].PassesFilter(filter)
		So(a, ShouldBeFalse)
		So(b, ShouldBeFalse)

		filter.UIDs = []uint32{103, 102, 101}
		a, b = expectedRootGUTs[2].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)

		filter.FTs = []summary.DirGUTFileType{summary.DGUTFileTypeTemp}
		a, b = expectedRootGUTs[3].PassesFilter(filter)
		So(a, ShouldBeFalse)
		So(b, ShouldBeFalse)
		a, b = expectedRootGUTs[0].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)

		filter.FTs = []summary.DirGUTFileType{summary.DGUTFileTypeTemp, summary.DGUTFileTypeCram}
		a, b = expectedRootGUTs[3].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)
		a, b = expectedRootGUTs[0].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeFalse)

		filter.UIDs = nil
		a, b = expectedRootGUTs[3].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)

		filter.GIDs = nil
		a, b = expectedRootGUTs[3].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)

		filter.FTs = []summary.DirGUTFileType{summary.DGUTFileTypeDir}
		a, b = expectedRootGUTs[1].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)
	})

	expectedUIDs := []uint32{101, 102}
	expectedGIDs := []uint32{1, 2}
	expectedFTs := []summary.DirGUTFileType{summary.DGUTFileTypeTemp,
		summary.DGUTFileTypeBam, summary.DGUTFileTypeCram, summary.DGUTFileTypeDir}

	const numDirectories = 10

	const directorySize = 1024

	Convey("GUTs can sum the count and size and provide UIDs, GIDs and FTs of their GUT elements", t, func() {
		c, s, a, m, u, g, t := expectedRootGUTs.Summary(nil)
		So(c, ShouldEqual, numDirectories+14)
		So(s, ShouldEqual, 85+numDirectories*directorySize)
		So(a, ShouldEqual, 50)
		So(m, ShouldEqual, 90)
		So(u, ShouldResemble, expectedUIDs)
		So(g, ShouldResemble, expectedGIDs)
		So(t, ShouldResemble, expectedFTs)
	})

	Convey("A DGUT can be encoded and decoded", t, func() {
		ch := new(codec.BincHandle)
		dirb, b := expected[0].encodeToBytes(ch)
		So(len(dirb), ShouldEqual, 1)
		So(len(b), ShouldEqual, 780)

		d := decodeDGUTbytes(ch, dirb, b)
		So(d, ShouldResemble, expected[0])
	})

	Convey("A DGUT can sum the count and size and provide UIDs, GIDs and FTs of its GUTs", t, func() {
		c, s, a, m, u, g, t := expected[0].Summary(nil)
		So(c, ShouldEqual, 14+numDirectories)
		So(s, ShouldEqual, 85+numDirectories*directorySize)
		So(a, ShouldEqual, 50)
		So(m, ShouldEqual, 90)
		So(u, ShouldResemble, expectedUIDs)
		So(g, ShouldResemble, expectedGIDs)
		So(t, ShouldResemble, expectedFTs)
	})

	Convey("Given multiline dgut data", t, func() {
		data := strings.NewReader(dgutData)

		Convey("You can parse it", func() {
			i := 0
			cb := func(dgut *DGUT) {
				So(alterDgutForTest(dgut), ShouldResemble, expected[i])

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
			paths, err := testMakeDBPaths(t)
			So(err, ShouldBeNil)

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

						c, s, a, m, u, g, t, _, errd := db.DirInfo("/", nil)
						So(errd, ShouldBeNil)
						So(c, ShouldEqual, 14+numDirectories)
						So(s, ShouldEqual, 85+numDirectories*directorySize)
						So(a, ShouldEqual, 50)
						So(m, ShouldEqual, 90)
						So(u, ShouldResemble, expectedUIDs)
						So(g, ShouldResemble, expectedGIDs)
						So(t, ShouldResemble, expectedFTs)

						c, s, a, m, u, g, t, _, errd = db.DirInfo("/a/c/d", nil)
						So(errd, ShouldBeNil)
						So(c, ShouldEqual, 6)
						So(s, ShouldEqual, 5+directorySize)
						So(a, ShouldEqual, 90)
						So(m, ShouldEqual, 90)
						So(u, ShouldResemble, []uint32{102})
						So(g, ShouldResemble, []uint32{2})
						So(t, ShouldResemble, []summary.DirGUTFileType{summary.DGUTFileTypeCram, summary.DGUTFileTypeDir})

						c, s, a, m, u, g, t, _, errd = db.DirInfo("/a/b/d/g", nil)
						So(errd, ShouldBeNil)
						So(c, ShouldEqual, 7)
						So(s, ShouldEqual, 60+directorySize)
						So(a, ShouldEqual, 60)
						So(m, ShouldEqual, 75)
						So(u, ShouldResemble, expectedUIDs)
						So(g, ShouldResemble, []uint32{1})
						So(t, ShouldResemble, []summary.DirGUTFileType{summary.DGUTFileTypeCram, summary.DGUTFileTypeDir})

						_, _, _, _, _, _, _, _, errd = db.DirInfo("/foo", nil)
						So(errd, ShouldNotBeNil)
						So(errd, ShouldEqual, ErrDirNotFound)

						c, s, a, m, u, g, t, _, errd = db.DirInfo("/", &Filter{GIDs: []uint32{1}})
						So(errd, ShouldBeNil)
						So(c, ShouldEqual, 9+8)
						So(s, ShouldEqual, 80+8*directorySize)
						So(a, ShouldEqual, 50)
						So(m, ShouldEqual, 80)
						So(u, ShouldResemble, expectedUIDs)
						So(g, ShouldResemble, []uint32{1})
						So(t, ShouldResemble, expectedFTs)

						c, s, a, m, u, g, t, _, errd = db.DirInfo("/", &Filter{UIDs: []uint32{102}})
						So(errd, ShouldBeNil)
						So(c, ShouldEqual, 9+2)
						So(s, ShouldEqual, 45+2*directorySize)
						So(a, ShouldEqual, 75)
						So(m, ShouldEqual, 90)
						So(u, ShouldResemble, []uint32{102})
						So(g, ShouldResemble, expectedGIDs)
						So(t, ShouldResemble, []summary.DirGUTFileType{summary.DGUTFileTypeCram, summary.DGUTFileTypeDir})

						c, s, a, m, u, g, t, _, errd = db.DirInfo("/", &Filter{GIDs: []uint32{1}, UIDs: []uint32{102}})
						So(errd, ShouldBeNil)
						So(c, ShouldEqual, 4)
						So(s, ShouldEqual, 40)
						So(a, ShouldEqual, 75)
						So(m, ShouldEqual, 75)
						So(u, ShouldResemble, []uint32{102})
						So(g, ShouldResemble, []uint32{1})
						So(t, ShouldResemble, []summary.DirGUTFileType{summary.DGUTFileTypeCram})

						c, s, a, m, u, g, t, _, errd = db.DirInfo("/", &Filter{
							GIDs: []uint32{1},
							UIDs: []uint32{102},
							FTs:  []summary.DirGUTFileType{summary.DGUTFileTypeTemp}})
						So(errd, ShouldBeNil)
						So(c, ShouldEqual, 0)
						So(s, ShouldEqual, 0)
						So(a, ShouldEqual, 0)
						So(m, ShouldEqual, 0)
						So(u, ShouldResemble, []uint32{})
						So(g, ShouldResemble, []uint32{})
						So(t, ShouldResemble, []summary.DirGUTFileType{})

						c, s, a, m, u, g, t, _, errd = db.DirInfo("/", &Filter{FTs: []summary.DirGUTFileType{summary.DGUTFileTypeTemp}})
						So(errd, ShouldBeNil)
						So(c, ShouldEqual, 1+1)
						So(s, ShouldEqual, 5+directorySize)
						So(a, ShouldEqual, 80)
						So(m, ShouldEqual, 80)
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
						data = strings.NewReader(encode.Base64Encode("/") +
							"\t3\t103\t7\t2\t2\t25\t25\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n" +
							encode.Base64Encode("/a/i") + "\t3\t103\t7\t1\t1\t25\t25\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n" +
							encode.Base64Encode("/i") + "\t3\t103\t7\t1\t1\t30\t30\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n")

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

							c, s, a, m, u, g, t, _, errd := db.DirInfo("/", nil)
							So(errd, ShouldBeNil)
							So(c, ShouldEqual, 16+numDirectories)
							So(s, ShouldEqual, 87+numDirectories*directorySize)
							So(a, ShouldEqual, 25)
							So(m, ShouldEqual, 90)
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

				Convey("You can get info on the database files", func() {
					info, err := db.Info()
					So(err, ShouldBeNil)
					So(info, ShouldResemble, &DBInfo{
						NumDirs:     11,
						NumDGUTs:    40,
						NumParents:  7,
						NumChildren: 10,
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

			paths, err := testMakeDBPaths(t)
			So(err, ShouldBeNil)

			db = NewDB(paths[0])

			err = os.WriteFile(paths[2], []byte("foo"), 0600)
			So(err, ShouldBeNil)

			err = db.Store(data, 4)
			So(err, ShouldNotBeNil)
		})
	})
}

// testData provides some test data and expected results.
func testData(t *testing.T) (dgutData string, expectedRootGUTs GUTs, expected []*DGUT, expectedKeys []string) {
	t.Helper()

	dgutData = internaldata.TestDGUTData(t, internaldata.CreateDefaultTestData(1, 2, 1, 101, 102))

	expectedRootGUTs = GUTs{
		{GID: 1, UID: 101, FT: summary.DGUTFileTypeTemp, Count: 2, Size: 1029, Atime: 80, Mtime: 80,
			SizeByAccessAge: [8]int64{5, 5, 5, 5, 5, 5, 5, 5},
			SizeByModifyAge: [8]int64{1029, 1029, 1029, 1029, 1029, 1029, 1029, 1029}},
		{GID: 1, UID: 101, FT: summary.DGUTFileTypeDir, Count: 8, Size: 8192, Atime: math.MaxInt, Mtime: 1,
			SizeByModifyAge: [8]int64{8192, 8192, 8192, 8192, 8192, 8192, 8192, 8192}},
		{GID: 1, UID: 101, FT: summary.DGUTFileTypeBam, Count: 2, Size: 10, Atime: 80, Mtime: 80,
			SizeByAccessAge: [8]int64{10, 10, 10, 10, 10, 10, 10, 10},
			SizeByModifyAge: [8]int64{10, 10, 10, 10, 10, 10, 10, 10}},
		{GID: 1, UID: 101, FT: summary.DGUTFileTypeCram, Count: 3, Size: 30, Atime: 50, Mtime: 60,
			SizeByAccessAge: [8]int64{30, 30, 30, 30, 30, 30, 30, 30},
			SizeByModifyAge: [8]int64{30, 30, 30, 30, 30, 30, 30, 30}},
		{GID: 1, UID: 102, FT: summary.DGUTFileTypeCram, Count: 4, Size: 40, Atime: 75, Mtime: 75,
			SizeByAccessAge: [8]int64{40, 40, 40, 40, 40, 40, 40, 40},
			SizeByModifyAge: [8]int64{40, 40, 40, 40, 40, 40, 40, 40}},
		{GID: 2, UID: 102, FT: summary.DGUTFileTypeDir, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1,
			SizeByModifyAge: [8]int64{2048, 2048, 2048, 2048, 2048, 2048, 2048, 2048}},
		{GID: 2, UID: 102, FT: summary.DGUTFileTypeCram, Count: 5, Size: 5, Atime: 90, Mtime: 90,
			SizeByAccessAge: [8]int64{5, 5, 5, 5, 5, 5, 5, 5},
			SizeByModifyAge: [8]int64{5, 5, 5, 5, 5, 5, 5, 5}},
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
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeTemp, Count: 2, Size: 1029, Atime: 80, Mtime: 80,
					SizeByAccessAge: [8]int64{5, 5, 5, 5, 5, 5, 5, 5},
					SizeByModifyAge: [8]int64{1029, 1029, 1029, 1029, 1029, 1029, 1029, 1029}},
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeDir, Count: 7, Size: 7168, Atime: math.MaxInt, Mtime: 1,
					SizeByModifyAge: [8]int64{7168, 7168, 7168, 7168, 7168, 7168, 7168, 7168}},
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeBam, Count: 2, Size: 10, Atime: 80, Mtime: 80,
					SizeByAccessAge: [8]int64{10, 10, 10, 10, 10, 10, 10, 10},
					SizeByModifyAge: [8]int64{10, 10, 10, 10, 10, 10, 10, 10}},
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeCram, Count: 3, Size: 30, Atime: 50, Mtime: 60,
					SizeByAccessAge: [8]int64{30, 30, 30, 30, 30, 30, 30, 30},
					SizeByModifyAge: [8]int64{30, 30, 30, 30, 30, 30, 30, 30}},
				{GID: 1, UID: 102, FT: summary.DGUTFileTypeCram, Count: 4, Size: 40, Atime: 75, Mtime: 75,
					SizeByAccessAge: [8]int64{40, 40, 40, 40, 40, 40, 40, 40},
					SizeByModifyAge: [8]int64{40, 40, 40, 40, 40, 40, 40, 40}},
			},
		},
		{
			Dir: "/a/b/d",
			GUTs: []*GUT{
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeDir, Count: 3, Size: 3072, Atime: math.MaxInt, Mtime: 1,
					SizeByModifyAge: [8]int64{3072, 3072, 3072, 3072, 3072, 3072, 3072, 3072}},
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeCram, Count: 3, Size: 30, Atime: 50, Mtime: 60,
					SizeByAccessAge: [8]int64{30, 30, 30, 30, 30, 30, 30, 30},
					SizeByModifyAge: [8]int64{30, 30, 30, 30, 30, 30, 30, 30}},
				{GID: 1, UID: 102, FT: summary.DGUTFileTypeCram, Count: 4, Size: 40, Atime: 75, Mtime: 75,
					SizeByAccessAge: [8]int64{40, 40, 40, 40, 40, 40, 40, 40},
					SizeByModifyAge: [8]int64{40, 40, 40, 40, 40, 40, 40, 40}},
			},
		},
		{
			Dir: "/a/b/d/f",
			GUTs: []*GUT{
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeDir, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1,
					SizeByModifyAge: [8]int64{1024, 1024, 1024, 1024, 1024, 1024, 1024, 1024}},
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeCram, Count: 1, Size: 10, Atime: 50, Mtime: 50,
					SizeByAccessAge: [8]int64{10, 10, 10, 10, 10, 10, 10, 10},
					SizeByModifyAge: [8]int64{10, 10, 10, 10, 10, 10, 10, 10}},
			},
		},
		{
			Dir: "/a/b/d/g",
			GUTs: []*GUT{
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeDir, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1,
					SizeByModifyAge: [8]int64{1024, 1024, 1024, 1024, 1024, 1024, 1024, 1024}},
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeCram, Count: 2, Size: 20, Atime: 60, Mtime: 60,
					SizeByAccessAge: [8]int64{20, 20, 20, 20, 20, 20, 20, 20},
					SizeByModifyAge: [8]int64{20, 20, 20, 20, 20, 20, 20, 20}},
				{GID: 1, UID: 102, FT: summary.DGUTFileTypeCram, Count: 4, Size: 40, Atime: 75, Mtime: 75,
					SizeByAccessAge: [8]int64{40, 40, 40, 40, 40, 40, 40, 40},
					SizeByModifyAge: [8]int64{40, 40, 40, 40, 40, 40, 40, 40}},
			},
		},
		{
			Dir: "/a/b/e",
			GUTs: []*GUT{
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeTemp, Count: 2, Size: 1029, Atime: 80, Mtime: 80,
					SizeByAccessAge: [8]int64{5, 5, 5, 5, 5, 5, 5, 5},
					SizeByModifyAge: [8]int64{1029, 1029, 1029, 1029, 1029, 1029, 1029, 1029}},
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeDir, Count: 3, Size: 3072, Atime: math.MaxInt, Mtime: 1,
					SizeByModifyAge: [8]int64{3072, 3072, 3072, 3072, 3072, 3072, 3072, 3072}},
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeBam, Count: 2, Size: 10, Atime: 80, Mtime: 80,
					SizeByAccessAge: [8]int64{10, 10, 10, 10, 10, 10, 10, 10},
					SizeByModifyAge: [8]int64{10, 10, 10, 10, 10, 10, 10, 10}},
			},
		},
		{
			Dir: "/a/b/e/h",
			GUTs: []*GUT{
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeTemp, Count: 2, Size: 1029, Atime: 80, Mtime: 80,
					SizeByAccessAge: [8]int64{5, 5, 5, 5, 5, 5, 5, 5},
					SizeByModifyAge: [8]int64{1029, 1029, 1029, 1029, 1029, 1029, 1029, 1029}},
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeDir, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1,
					SizeByModifyAge: [8]int64{2048, 2048, 2048, 2048, 2048, 2048, 2048, 2048}},
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeBam, Count: 2, Size: 10, Atime: 80, Mtime: 80,
					SizeByAccessAge: [8]int64{10, 10, 10, 10, 10, 10, 10, 10},
					SizeByModifyAge: [8]int64{10, 10, 10, 10, 10, 10, 10, 10}},
			},
		},
		{
			Dir: "/a/b/e/h/tmp",
			GUTs: []*GUT{
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeTemp, Count: 2, Size: 1029, Atime: 80, Mtime: 80,
					SizeByAccessAge: [8]int64{5, 5, 5, 5, 5, 5, 5, 5},
					SizeByModifyAge: [8]int64{1029, 1029, 1029, 1029, 1029, 1029, 1029, 1029}},
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeDir, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1,
					SizeByModifyAge: [8]int64{1024, 1024, 1024, 1024, 1024, 1024, 1024, 1024}},
				{GID: 1, UID: 101, FT: summary.DGUTFileTypeBam, Count: 1, Size: 5, Atime: 80, Mtime: 80,
					SizeByAccessAge: [8]int64{5, 5, 5, 5, 5, 5, 5, 5},
					SizeByModifyAge: [8]int64{5, 5, 5, 5, 5, 5, 5, 5}},
			},
		},
		{
			Dir: "/a/c",
			GUTs: []*GUT{
				{GID: 2, UID: 102, FT: summary.DGUTFileTypeDir, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1,
					SizeByModifyAge: [8]int64{2048, 2048, 2048, 2048, 2048, 2048, 2048, 2048}},
				{GID: 2, UID: 102, FT: summary.DGUTFileTypeCram, Count: 5, Size: 5, Atime: 90, Mtime: 90,
					SizeByAccessAge: [8]int64{5, 5, 5, 5, 5, 5, 5, 5},
					SizeByModifyAge: [8]int64{5, 5, 5, 5, 5, 5, 5, 5}},
			},
		},
		{
			Dir: "/a/c/d",
			GUTs: []*GUT{
				{GID: 2, UID: 102, FT: summary.DGUTFileTypeDir, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1,
					SizeByModifyAge: [8]int64{1024, 1024, 1024, 1024, 1024, 1024, 1024, 1024}},
				{GID: 2, UID: 102, FT: summary.DGUTFileTypeCram, Count: 5, Size: 5, Atime: 90, Mtime: 90,
					SizeByAccessAge: [8]int64{5, 5, 5, 5, 5, 5, 5, 5},
					SizeByModifyAge: [8]int64{5, 5, 5, 5, 5, 5, 5, 5}},
			},
		},
	}

	expectedKeys = []string{"/", "/a", "/a/b", "/a/b/d", "/a/b/d/f",
		"/a/b/d/g", "/a/b/e", "/a/b/e/h", "/a/b/e/h/tmp", "/a/c", "/a/c/d"}

	return dgutData, expectedRootGUTs, expected, expectedKeys
}

// testMakeDBPaths creates a temp dir that will be cleaned up automatically, and
// returns the paths to the directory and dgut and children database files
// inside that would be created. The files aren't actually created.
func testMakeDBPaths(t *testing.T) ([]string, error) {
	t.Helper()

	dir := t.TempDir()

	set, err := newDBSet(dir)
	if err != nil {
		return nil, err
	}

	paths := set.paths()

	return append([]string{dir}, paths...), nil
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

func alterDgutForTest(dgut *DGUT) *DGUT {
	for _, gut := range dgut.GUTs {
		if gut.FT == summary.DGUTFileTypeDir {
			gut.Atime = math.MaxInt
		}
	}

	return dgut
}
