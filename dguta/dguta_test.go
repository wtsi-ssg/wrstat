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

package dguta

import (
	"math"
	"os"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/ugorji/go/codec"
	internaldata "github.com/wtsi-ssg/wrstat/v5/internal/data"
	"github.com/wtsi-ssg/wrstat/v5/internal/encode"
	"github.com/wtsi-ssg/wrstat/v5/summary"
	bolt "go.etcd.io/bbolt"
)

func TestDGUTA(t *testing.T) {
	Convey("You can parse a single line of dguta data", t, func() {
		line := encode.Base64Encode("/") + "\t1\t101\t0\t0\t3\t30\t50\t50\n"
		dir, gut, err := parseDGUTALine(line)
		So(err, ShouldBeNil)
		So(dir, ShouldEqual, "/")
		So(gut, ShouldResemble, &GUTA{GID: 1, UID: 101, FT: summary.DGUTAFileTypeOther,
			Age: summary.DGUTAgeAll, Count: 3, Size: 30, Atime: 50, Mtime: 50})

		Convey("But invalid data won't parse", func() {
			_, _, err = parseDGUTALine(encode.Base64Encode("/") +
				"\t1\t101\t0\t0\t3\t50\t50\n")

			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTALine(encode.Base64Encode("/") +
				"\tfoo\t101\t0\t0\t3\t30\t50\t50\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTALine(encode.Base64Encode("/") +
				"\t1\tfoo\t0\t0\t3\t30\t50\t50\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTALine(encode.Base64Encode("/") +
				"\t1\t101\tfoo\t0\t3\t30\t50\t50\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTALine(encode.Base64Encode("/") +
				"\t1\t101\t0\tfoo\t3\t30\t50\t50\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTALine(encode.Base64Encode("/") +
				"\t1\t101\t0\t0\tfoo\t30\t50\t50\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTALine(encode.Base64Encode("/") +
				"\t1\t101\t0\t0\t3\tfoo\t50\t50\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTALine(encode.Base64Encode("/") +
				"\t1\t101\t0\t0\t3\t30\tfoo\t50\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			_, _, err = parseDGUTALine(encode.Base64Encode("/") +
				"\t1\t101\t0\t0\t3\t30\t50\tfoo\n")
			So(err, ShouldEqual, ErrInvalidFormat)

			So(err.Error(), ShouldEqual, "the provided data was not in dguta format")

			_, _, err = parseDGUTALine("\t\t\t\t\t\t\t\t\n")
			So(err, ShouldEqual, ErrBlankLine)

			So(err.Error(), ShouldEqual, "the provided line had no information")
		})
	})

	dgutData, expectedRootGUTs, expected, expectedKeys := testData(t)

	Convey("You can see if a GUT passes a filter", t, func() {
		numGutas := 17
		emptyGutas := 8
		testIndex := func(index int) int {
			if index > 5 {
				return index*numGutas - emptyGutas*2
			} else if index > 1 {
				return index*numGutas - emptyGutas
			}

			return index * numGutas
		}

		filter := &Filter{}
		a, b := expectedRootGUTs[testIndex(3)].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)

		a, b = expectedRootGUTs[0].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeFalse)

		filter.GIDs = []uint32{3, 4, 5}
		a, b = expectedRootGUTs[testIndex(3)].PassesFilter(filter)
		So(a, ShouldBeFalse)
		So(b, ShouldBeFalse)

		filter.GIDs = []uint32{3, 2, 1}
		a, b = expectedRootGUTs[testIndex(3)].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)

		filter.UIDs = []uint32{103}
		a, b = expectedRootGUTs[testIndex(3)].PassesFilter(filter)
		So(a, ShouldBeFalse)
		So(b, ShouldBeFalse)

		filter.UIDs = []uint32{103, 102, 101}
		a, b = expectedRootGUTs[testIndex(2)].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)

		filter.FTs = []summary.DirGUTAFileType{summary.DGUTAFileTypeTemp}
		a, b = expectedRootGUTs[testIndex(3)].PassesFilter(filter)
		So(a, ShouldBeFalse)
		So(b, ShouldBeFalse)
		a, b = expectedRootGUTs[0].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)

		filter.FTs = []summary.DirGUTAFileType{summary.DGUTAFileTypeTemp, summary.DGUTAFileTypeCram}
		a, b = expectedRootGUTs[testIndex(3)].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)
		a, b = expectedRootGUTs[0].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeFalse)

		filter.UIDs = nil
		a, b = expectedRootGUTs[testIndex(3)].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)

		filter.GIDs = nil
		a, b = expectedRootGUTs[testIndex(3)].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)

		filter.FTs = []summary.DirGUTAFileType{summary.DGUTAFileTypeDir}
		a, b = expectedRootGUTs[testIndex(1)].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)

		filter = &Filter{Age: summary.DGUTAgeA1M}
		a, b = expectedRootGUTs[testIndex(7)+1].PassesFilter(filter)
		So(a, ShouldBeTrue)
		So(b, ShouldBeTrue)

		filter.Age = summary.DGUTAgeA7Y
		a, b = expectedRootGUTs[testIndex(7)+1].PassesFilter(filter)
		So(a, ShouldBeFalse)
		So(b, ShouldBeFalse)
	})

	expectedUIDs := []uint32{101, 102, 103}
	expectedGIDs := []uint32{1, 2, 3}
	expectedFTs := []summary.DirGUTAFileType{summary.DGUTAFileTypeTemp,
		summary.DGUTAFileTypeBam, summary.DGUTAFileTypeCram, summary.DGUTAFileTypeDir}

	const numDirectories = 10

	const directorySize = 1024

	expectedMtime := time.Unix(time.Now().Unix()-(summary.SecondsInAYear*3), 0)

	defaultFilter := &Filter{Age: summary.DGUTAgeAll}

	Convey("GUTs can sum the count and size and provide UIDs, GIDs and FTs of their GUT elements", t, func() {
		ds := expectedRootGUTs.Summary(defaultFilter)
		So(ds.Count, ShouldEqual, 21+numDirectories)
		So(ds.Size, ShouldEqual, 92+numDirectories*directorySize)
		So(ds.Atime, ShouldEqual, time.Unix(50, 0))
		So(ds.Mtime, ShouldHappenBetween, expectedMtime.Add(-5*time.Second), expectedMtime.Add(5*time.Second))
		So(ds.UIDs, ShouldResemble, expectedUIDs)
		So(ds.GIDs, ShouldResemble, expectedGIDs)
		So(ds.FTs, ShouldResemble, expectedFTs)
	})

	Convey("A DGUTA can be encoded and decoded", t, func() {
		ch := new(codec.BincHandle)
		dirb, b := expected[0].encodeToBytes(ch)
		So(len(dirb), ShouldEqual, 1)
		So(len(b), ShouldEqual, 5964)

		d := decodeDGUTAbytes(ch, dirb, b)
		So(d, ShouldResemble, expected[0])
	})

	Convey("A DGUTA can sum the count and size and provide UIDs, GIDs and FTs of its GUTs", t, func() {
		ds := expected[0].Summary(defaultFilter)
		So(ds.Count, ShouldEqual, 21+numDirectories)
		So(ds.Size, ShouldEqual, 92+numDirectories*directorySize)
		So(ds.Atime, ShouldEqual, time.Unix(50, 0))
		So(ds.Mtime, ShouldHappenBetween, expectedMtime.Add(-5*time.Second), expectedMtime.Add(5*time.Second))
		So(ds.UIDs, ShouldResemble, expectedUIDs)
		So(ds.GIDs, ShouldResemble, expectedGIDs)
		So(ds.FTs, ShouldResemble, expectedFTs)
	})

	Convey("Given multiline dguta data", t, func() {
		data := strings.NewReader(dgutData)

		Convey("You can parse it", func() {
			i := 0
			cb := func(dguta *DGUTA) {
				So(alterDgutaForTest(dguta), ShouldResemble, expected[i])

				i++
			}

			err := parseDGUTALines(data, cb)
			So(err, ShouldBeNil)
			So(i, ShouldEqual, 11)
		})

		Convey("You can't parse invalid data", func() {
			data = strings.NewReader("foo")
			i := 0
			cb := func(dguta *DGUTA) {
				i++
			}

			err := parseDGUTALines(data, cb)
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

					keys, errt := testGetDBKeys(paths[1], gutaBucket)
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

						ds, errd := db.DirInfo("/", defaultFilter)
						So(errd, ShouldBeNil)
						So(ds.Count, ShouldEqual, 21+numDirectories)
						So(ds.Size, ShouldEqual, 92+numDirectories*directorySize)
						So(ds.Atime, ShouldEqual, time.Unix(50, 0))
						So(ds.Mtime, ShouldHappenBetween, expectedMtime.Add(-5*time.Second), expectedMtime.Add(5*time.Second))
						So(ds.UIDs, ShouldResemble, expectedUIDs)
						So(ds.GIDs, ShouldResemble, expectedGIDs)
						So(ds.FTs, ShouldResemble, expectedFTs)

						ds, errd = db.DirInfo("/a/c/d", defaultFilter)
						So(errd, ShouldBeNil)
						So(ds.Count, ShouldEqual, 13)
						So(ds.Size, ShouldEqual, 12+directorySize)
						So(ds.Atime, ShouldEqual, time.Unix(90, 0))
						So(ds.Mtime, ShouldHappenBetween, expectedMtime.Add(-5*time.Second), expectedMtime.Add(5*time.Second))
						So(ds.UIDs, ShouldResemble, []uint32{102, 103})
						So(ds.GIDs, ShouldResemble, []uint32{2, 3})
						So(ds.FTs, ShouldResemble, []summary.DirGUTAFileType{summary.DGUTAFileTypeCram, summary.DGUTAFileTypeDir})

						ds, errd = db.DirInfo("/a/b/d/g", defaultFilter)
						So(errd, ShouldBeNil)
						So(ds.Count, ShouldEqual, 7)
						So(ds.Size, ShouldEqual, 60+directorySize)
						So(ds.Atime, ShouldEqual, time.Unix(60, 0))
						So(ds.Mtime, ShouldEqual, time.Unix(75, 0))
						So(ds.UIDs, ShouldResemble, []uint32{101, 102})
						So(ds.GIDs, ShouldResemble, []uint32{1})
						So(ds.FTs, ShouldResemble, []summary.DirGUTAFileType{summary.DGUTAFileTypeCram, summary.DGUTAFileTypeDir})

						_, errd = db.DirInfo("/foo", defaultFilter)
						So(errd, ShouldNotBeNil)
						So(errd, ShouldEqual, ErrDirNotFound)

						ds, errd = db.DirInfo("/", &Filter{GIDs: []uint32{1}})
						So(errd, ShouldBeNil)
						So(ds.Count, ShouldEqual, 17)
						So(ds.Size, ShouldEqual, 8272)
						So(ds.Atime, ShouldEqual, time.Unix(50, 0))
						So(ds.Mtime, ShouldEqual, time.Unix(80, 0))
						So(ds.UIDs, ShouldResemble, []uint32{101, 102})
						So(ds.GIDs, ShouldResemble, []uint32{1})
						So(ds.FTs, ShouldResemble, expectedFTs)

						ds, errd = db.DirInfo("/", &Filter{UIDs: []uint32{102}})
						So(errd, ShouldBeNil)
						So(ds.Count, ShouldEqual, 11)
						So(ds.Size, ShouldEqual, 2093)
						So(ds.Atime, ShouldEqual, time.Unix(75, 0))
						So(ds.Mtime, ShouldEqual, time.Unix(90, 0))
						So(ds.UIDs, ShouldResemble, []uint32{102})
						So(ds.GIDs, ShouldResemble, []uint32{1, 2})
						So(ds.FTs, ShouldResemble, []summary.DirGUTAFileType{summary.DGUTAFileTypeCram, summary.DGUTAFileTypeDir})

						ds, errd = db.DirInfo("/", &Filter{GIDs: []uint32{1}, UIDs: []uint32{102}})
						So(errd, ShouldBeNil)
						So(ds.Count, ShouldEqual, 4)
						So(ds.Size, ShouldEqual, 40)
						So(ds.Atime, ShouldEqual, time.Unix(75, 0))
						So(ds.Mtime, ShouldEqual, time.Unix(75, 0))
						So(ds.UIDs, ShouldResemble, []uint32{102})
						So(ds.GIDs, ShouldResemble, []uint32{1})
						So(ds.FTs, ShouldResemble, []summary.DirGUTAFileType{summary.DGUTAFileTypeCram})

						ds, errd = db.DirInfo("/", &Filter{
							GIDs: []uint32{1},
							UIDs: []uint32{102},
							FTs:  []summary.DirGUTAFileType{summary.DGUTAFileTypeTemp}})
						So(errd, ShouldBeNil)
						So(ds.Count, ShouldEqual, 0)
						So(ds.Size, ShouldEqual, 0)
						So(ds.Atime, ShouldEqual, time.Unix(0, 0))
						So(ds.Mtime, ShouldEqual, time.Unix(0, 0))
						So(ds.UIDs, ShouldResemble, []uint32{})
						So(ds.GIDs, ShouldResemble, []uint32{})
						So(ds.FTs, ShouldResemble, []summary.DirGUTAFileType{})

						ds, errd = db.DirInfo("/", &Filter{FTs: []summary.DirGUTAFileType{summary.DGUTAFileTypeTemp}})
						So(errd, ShouldBeNil)
						So(ds.Count, ShouldEqual, 2)
						So(ds.Size, ShouldEqual, 5+directorySize)
						So(ds.Atime, ShouldEqual, time.Unix(80, 0))
						So(ds.Mtime, ShouldEqual, time.Unix(80, 0))
						So(ds.UIDs, ShouldResemble, []uint32{101})
						So(ds.GIDs, ShouldResemble, []uint32{1})
						So(ds.FTs, ShouldResemble, []summary.DirGUTAFileType{summary.DGUTAFileTypeTemp})

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
							"\t3\t103\t7\t0\t2\t2\t25\t25\n" +
							encode.Base64Encode("/a/i") + "\t3\t103\t7\t0\t1\t1\t25\t25\n" +
							encode.Base64Encode("/i") + "\t3\t103\t7\t0\t1\t1\t30\t30\n")

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

							ds, errd := db.DirInfo("/", nil)
							So(errd, ShouldBeNil)
							So(ds.Count, ShouldEqual, 407)
							So(ds.Size, ShouldEqual, 93684)
							So(ds.Atime, ShouldEqual, time.Unix(25, 0))
							So(ds.Mtime, ShouldHappenBetween, expectedMtime.Add(-5*time.Second), expectedMtime.Add(5*time.Second))
							So(ds.UIDs, ShouldResemble, []uint32{101, 102, 103})
							So(ds.GIDs, ShouldResemble, []uint32{1, 2, 3})
							So(ds.FTs, ShouldResemble, expectedFTs)

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
						NumDGUTAs:   620,
						NumParents:  7,
						NumChildren: 10,
					})
				})
			})

			Convey("Storing with a batch size == directories works", func() {
				err := db.Store(data, len(expectedKeys))
				So(err, ShouldBeNil)

				keys, errt := testGetDBKeys(paths[1], gutaBucket)
				So(errt, ShouldBeNil)
				So(keys, ShouldResemble, expectedKeys)
			})

			Convey("Storing with a batch size > directories works", func() {
				err := db.Store(data, len(expectedKeys)+2)
				So(err, ShouldBeNil)

				keys, errt := testGetDBKeys(paths[1], gutaBucket)
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
					err = db.writeSet.dgutas.Close()
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

					err = db.writeSet.dgutas.View(db.storeDGUTAs)
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
func testData(t *testing.T) (dgutData string, expectedRootGUTs GUTAs, expected []*DGUTA, expectedKeys []string) {
	t.Helper()

	dgutData = internaldata.TestDGUTAData(t, internaldata.CreateDefaultTestData(1, 2, 1, 101, 102))

	expectedRootGUTs = GUTAs{
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeAll, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA1M, Count: 1, Size: 5, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM2M, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM6M, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM1Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM2Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM3Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM5Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM7Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA2M, Count: 1, Size: 5, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA6M, Count: 1, Size: 5, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA1Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA2Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA3Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA5Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA7Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM1M, Count: 2, Size: 1029, Atime: 80, Mtime: 80},

		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeAll, Count: 8, Size: 8192, Atime: math.MaxInt, Mtime: 1},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM2M, Count: 8, Size: 8192, Atime: math.MaxInt, Mtime: 1},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM6M, Count: 8, Size: 8192, Atime: math.MaxInt, Mtime: 1},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM1Y, Count: 8, Size: 8192, Atime: math.MaxInt, Mtime: 1},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM2Y, Count: 8, Size: 8192, Atime: math.MaxInt, Mtime: 1},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM3Y, Count: 8, Size: 8192, Atime: math.MaxInt, Mtime: 1},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM5Y, Count: 8, Size: 8192, Atime: math.MaxInt, Mtime: 1},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM7Y, Count: 8, Size: 8192, Atime: math.MaxInt, Mtime: 1},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM1M, Count: 8, Size: 8192, Atime: math.MaxInt, Mtime: 1},

		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeAll, Count: 2, Size: 10, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA1M, Count: 2, Size: 10, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM2M, Count: 2, Size: 10, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM6M, Count: 2, Size: 10, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM1Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM2Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM3Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM5Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM7Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA2M, Count: 2, Size: 10, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA6M, Count: 2, Size: 10, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA1Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA2Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA3Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA5Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA7Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM1M, Count: 2, Size: 10, Atime: 80, Mtime: 80},

		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeAll, Count: 3, Size: 30, Atime: 50, Mtime: 60},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1M, Count: 3, Size: 30, Atime: 50, Mtime: 60},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2M, Count: 3, Size: 30, Atime: 50, Mtime: 60},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM6M, Count: 3, Size: 30, Atime: 50, Mtime: 60},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM3Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM5Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM7Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2M, Count: 3, Size: 30, Atime: 50, Mtime: 60},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA6M, Count: 3, Size: 30, Atime: 50, Mtime: 60},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA3Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA5Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA7Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
		{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1M, Count: 3, Size: 30, Atime: 50, Mtime: 60},

		{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeAll, Count: 4, Size: 40, Atime: 75, Mtime: 75},
		{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
		{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
		{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM6M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
		{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
		{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
		{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM3Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
		{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM5Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
		{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM7Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
		{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
		{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA6M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
		{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
		{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
		{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA3Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
		{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA5Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
		{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA7Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
		{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1M, Count: 4, Size: 40, Atime: 75, Mtime: 75},

		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeAll, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM2M, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM6M, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM1Y, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM2Y, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM3Y, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM5Y, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM7Y, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM1M, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},

		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeAll, Count: 5, Size: 5, Atime: 90, Mtime: 90},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1M, Count: 5, Size: 5, Atime: 90, Mtime: 90},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2M, Count: 5, Size: 5, Atime: 90, Mtime: 90},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM6M, Count: 5, Size: 5, Atime: 90, Mtime: 90},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM3Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM5Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM7Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2M, Count: 5, Size: 5, Atime: 90, Mtime: 90},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA6M, Count: 5, Size: 5, Atime: 90, Mtime: 90},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA3Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA5Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA7Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
		{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1M, Count: 5, Size: 5, Atime: 90, Mtime: 90},

		{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeAll, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
		{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1M, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
		{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2M, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
		{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM6M, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
		{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1Y, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
		{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2Y, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
		{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM3Y, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
		{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2M, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
		{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA6M, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
		{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1Y, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
		{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1M, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
	}

	expected = []*DGUTA{
		{
			Dir:   "/",
			GUTAs: expectedRootGUTs,
		},
		{
			Dir:   "/a",
			GUTAs: expectedRootGUTs,
		},
		{
			Dir: "/a/b",
			GUTAs: []*GUTA{
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeAll, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA1M, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM2M, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM6M, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM1Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM2Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM3Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM5Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM7Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA2M, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA6M, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA1Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA2Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA3Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA5Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA7Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM1M, Count: 2, Size: 1029, Atime: 80, Mtime: 80},

				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeAll, Count: 7, Size: 7168, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM2M, Count: 7, Size: 7168, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM6M, Count: 7, Size: 7168, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM1Y, Count: 7, Size: 7168, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM2Y, Count: 7, Size: 7168, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM3Y, Count: 7, Size: 7168, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM5Y, Count: 7, Size: 7168, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM7Y, Count: 7, Size: 7168, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM1M, Count: 7, Size: 7168, Atime: math.MaxInt, Mtime: 1},

				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeAll, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA1M, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM2M, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM6M, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM1Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM2Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM3Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM5Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM7Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA2M, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA6M, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA1Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA2Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA3Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA5Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA7Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM1M, Count: 2, Size: 10, Atime: 80, Mtime: 80},

				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeAll, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1M, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2M, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM6M, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM3Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM5Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM7Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2M, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA6M, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA3Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA5Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA7Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1M, Count: 3, Size: 30, Atime: 50, Mtime: 60},

				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeAll, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM6M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM3Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM5Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM7Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA6M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA3Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA5Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA7Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
			},
		},
		{
			Dir: "/a/b/d",
			GUTAs: []*GUTA{
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeAll, Count: 3, Size: 3072, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM2M, Count: 3, Size: 3072, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM6M, Count: 3, Size: 3072, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM1Y, Count: 3, Size: 3072, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM2Y, Count: 3, Size: 3072, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM3Y, Count: 3, Size: 3072, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM5Y, Count: 3, Size: 3072, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM7Y, Count: 3, Size: 3072, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM1M, Count: 3, Size: 3072, Atime: math.MaxInt, Mtime: 1},

				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeAll, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1M, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2M, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM6M, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM3Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM5Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM7Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2M, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA6M, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA3Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA5Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA7Y, Count: 3, Size: 30, Atime: 50, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1M, Count: 3, Size: 30, Atime: 50, Mtime: 60},

				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeAll, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM6M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM3Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM5Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM7Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA6M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA3Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA5Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA7Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
			},
		},
		{
			Dir: "/a/b/d/f",
			GUTAs: []*GUTA{
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeAll, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM2M, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM6M, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM1Y, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM2Y, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM3Y, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM5Y, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM7Y, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM1M, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},

				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeAll, Count: 1, Size: 10, Atime: 50, Mtime: 50},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1M, Count: 1, Size: 10, Atime: 50, Mtime: 50},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2M, Count: 1, Size: 10, Atime: 50, Mtime: 50},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM6M, Count: 1, Size: 10, Atime: 50, Mtime: 50},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1Y, Count: 1, Size: 10, Atime: 50, Mtime: 50},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2Y, Count: 1, Size: 10, Atime: 50, Mtime: 50},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM3Y, Count: 1, Size: 10, Atime: 50, Mtime: 50},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM5Y, Count: 1, Size: 10, Atime: 50, Mtime: 50},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM7Y, Count: 1, Size: 10, Atime: 50, Mtime: 50},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2M, Count: 1, Size: 10, Atime: 50, Mtime: 50},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA6M, Count: 1, Size: 10, Atime: 50, Mtime: 50},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1Y, Count: 1, Size: 10, Atime: 50, Mtime: 50},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2Y, Count: 1, Size: 10, Atime: 50, Mtime: 50},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA3Y, Count: 1, Size: 10, Atime: 50, Mtime: 50},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA5Y, Count: 1, Size: 10, Atime: 50, Mtime: 50},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA7Y, Count: 1, Size: 10, Atime: 50, Mtime: 50},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1M, Count: 1, Size: 10, Atime: 50, Mtime: 50},
			},
		},
		{
			Dir: "/a/b/d/g",
			GUTAs: []*GUTA{
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeAll, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM2M, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM6M, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM1Y, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM2Y, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM3Y, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM5Y, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM7Y, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM1M, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},

				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeAll, Count: 2, Size: 20, Atime: 60, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1M, Count: 2, Size: 20, Atime: 60, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2M, Count: 2, Size: 20, Atime: 60, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM6M, Count: 2, Size: 20, Atime: 60, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1Y, Count: 2, Size: 20, Atime: 60, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2Y, Count: 2, Size: 20, Atime: 60, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM3Y, Count: 2, Size: 20, Atime: 60, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM5Y, Count: 2, Size: 20, Atime: 60, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM7Y, Count: 2, Size: 20, Atime: 60, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2M, Count: 2, Size: 20, Atime: 60, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA6M, Count: 2, Size: 20, Atime: 60, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1Y, Count: 2, Size: 20, Atime: 60, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2Y, Count: 2, Size: 20, Atime: 60, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA3Y, Count: 2, Size: 20, Atime: 60, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA5Y, Count: 2, Size: 20, Atime: 60, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA7Y, Count: 2, Size: 20, Atime: 60, Mtime: 60},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1M, Count: 2, Size: 20, Atime: 60, Mtime: 60},

				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeAll, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM6M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM3Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM5Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM7Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA6M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA3Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA5Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA7Y, Count: 4, Size: 40, Atime: 75, Mtime: 75},
				{GID: 1, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1M, Count: 4, Size: 40, Atime: 75, Mtime: 75},
			},
		},
		{
			Dir: "/a/b/e",
			GUTAs: []*GUTA{
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeAll, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA1M, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM2M, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM6M, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM1Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM2Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM3Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM5Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM7Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA2M, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA6M, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA1Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA2Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA3Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA5Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA7Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM1M, Count: 2, Size: 1029, Atime: 80, Mtime: 80},

				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeAll, Count: 3, Size: 3072, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM2M, Count: 3, Size: 3072, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM6M, Count: 3, Size: 3072, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM1Y, Count: 3, Size: 3072, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM2Y, Count: 3, Size: 3072, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM3Y, Count: 3, Size: 3072, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM5Y, Count: 3, Size: 3072, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM7Y, Count: 3, Size: 3072, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM1M, Count: 3, Size: 3072, Atime: math.MaxInt, Mtime: 1},

				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeAll, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA1M, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM2M, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM6M, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM1Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM2Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM3Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM5Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM7Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA2M, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA6M, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA1Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA2Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA3Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA5Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA7Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM1M, Count: 2, Size: 10, Atime: 80, Mtime: 80},
			},
		},
		{
			Dir: "/a/b/e/h",
			GUTAs: []*GUTA{

				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeAll, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA1M, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM2M, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM6M, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM1Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM2Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM3Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM5Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM7Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA2M, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA6M, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA1Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA2Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA3Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA5Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA7Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM1M, Count: 2, Size: 1029, Atime: 80, Mtime: 80},

				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeAll, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM2M, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM6M, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM1Y, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM2Y, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM3Y, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM5Y, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM7Y, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM1M, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},

				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeAll, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA1M, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM2M, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM6M, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM1Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM2Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM3Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM5Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM7Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA2M, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA6M, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA1Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA2Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA3Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA5Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA7Y, Count: 2, Size: 10, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM1M, Count: 2, Size: 10, Atime: 80, Mtime: 80},
			},
		},
		{
			Dir: "/a/b/e/h/tmp",
			GUTAs: []*GUTA{
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeAll, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA1M, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM2M, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM6M, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM1Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM2Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM3Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM5Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM7Y, Count: 2, Size: 1029, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA2M, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA6M, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA1Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA2Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA3Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA5Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeA7Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeTemp, Age: summary.DGUTAgeM1M, Count: 2, Size: 1029, Atime: 80, Mtime: 80},

				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeAll, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM2M, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM6M, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM1Y, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM2Y, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM3Y, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM5Y, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM7Y, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM1M, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},

				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeAll, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA1M, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM2M, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM6M, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM1Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM2Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM3Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM5Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM7Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA2M, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA6M, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA1Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA2Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA3Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA5Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeA7Y, Count: 1, Size: 5, Atime: 80, Mtime: 80},
				{GID: 1, UID: 101, FT: summary.DGUTAFileTypeBam, Age: summary.DGUTAgeM1M, Count: 1, Size: 5, Atime: 80, Mtime: 80},
			},
		},
		{
			Dir: "/a/c",
			GUTAs: []*GUTA{
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeAll, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM2M, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM6M, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM1Y, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM2Y, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM3Y, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM5Y, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM7Y, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM1M, Count: 2, Size: 2048, Atime: math.MaxInt, Mtime: 1},

				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeAll, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1M, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2M, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM6M, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM3Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM5Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM7Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2M, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA6M, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA3Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA5Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA7Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1M, Count: 5, Size: 5, Atime: 90, Mtime: 90},

				{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeAll, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
				{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1M, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
				{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2M, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
				{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM6M, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
				{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1Y, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
				{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2Y, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
				{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM3Y, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
				{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2M, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
				{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA6M, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
				{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1Y, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
				{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1M, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
			},
		},
		{
			Dir: "/a/c/d",
			GUTAs: []*GUTA{
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeAll, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM2M, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM6M, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM1Y, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM2Y, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM3Y, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM5Y, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM7Y, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeDir, Age: summary.DGUTAgeM1M, Count: 1, Size: 1024, Atime: math.MaxInt, Mtime: 1},

				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeAll, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1M, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2M, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM6M, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM3Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM5Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM7Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2M, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA6M, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA3Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA5Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA7Y, Count: 5, Size: 5, Atime: 90, Mtime: 90},
				{GID: 2, UID: 102, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1M, Count: 5, Size: 5, Atime: 90, Mtime: 90},

				{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeAll, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
				{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1M, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
				{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2M, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
				{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM6M, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
				{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1Y, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
				{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM2Y, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
				{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM3Y, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
				{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA2M, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
				{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA6M, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
				{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeA1Y, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
				{GID: 3, UID: 103, FT: summary.DGUTAFileTypeCram, Age: summary.DGUTAgeM1M, Count: 7, Size: 7, Atime: time.Now().Unix() - summary.SecondsInAYear, Mtime: time.Now().Unix() - (summary.SecondsInAYear * 3)},
			},
		},
	}

	expectedKeys = []string{"/", "/a", "/a/b", "/a/b/d", "/a/b/d/f",
		"/a/b/d/g", "/a/b/e", "/a/b/e/h", "/a/b/e/h/tmp", "/a/c", "/a/c/d"}

	return dgutData, expectedRootGUTs, expected, expectedKeys
}

// testMakeDBPaths creates a temp dir that will be cleaned up automatically, and
// returns the paths to the directory and dguta and children database files
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

func alterDgutaForTest(dguta *DGUTA) *DGUTA {
	for _, guta := range dguta.GUTAs {
		if guta.FT == summary.DGUTAFileTypeDir && guta.Count > 0 {
			guta.Atime = math.MaxInt
		}
	}

	return dguta
}
