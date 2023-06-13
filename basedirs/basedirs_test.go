/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
 *   Michael Woolnough <mw31@sanger.ac.uk>
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

package basedirs

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/v4/dgut"
	"github.com/wtsi-ssg/wrstat/v4/internal/fixtimes"
	"github.com/wtsi-ssg/wrstat/v4/summary"
)

func TestBaseDirs(t *testing.T) {
	csvPath := makeQuotasCSV(t, `1,/lustre/scratch125,200,20
2,/lustre/scratch125,300,30
2,/lustre/scratch123,400,40
3,/lustre/scratch125,500,50
`)

	Convey("Given a Tree and Quotas you can make a BaseDirs", t, func() {
		tree, locDirs := baseDirsTree(t)
		projectA := locDirs[0]
		projectB125 := locDirs[1]
		projectB123 := locDirs[2]
		projectC1 := locDirs[3]
		user2 := locDirs[5]

		quotas, err := ParseQuotas(csvPath)
		So(err, ShouldBeNil)

		dir := t.TempDir()
		dbPath := filepath.Join(dir, "basedir.db")

		bd, err := NewCreator(dbPath, tree, quotas)
		So(err, ShouldBeNil)
		So(bd, ShouldNotBeNil)

		bd.mountPoints = mountPoints{
			"/lustre/scratch123/",
			"/lustre/scratch125/",
		}

		Convey("With which you can calculate base directories", func() {
			expectedAtime := time.Unix(50, 0)
			expectedMtime := time.Unix(50, 0)
			expectedMtimeA := time.Unix(100, 0)
			expectedFTsBam := []summary.DirGUTFileType{summary.DGUTFileTypeBam}

			Convey("of each group", func() { //nolint:dupl
				dcss, err := bd.CalculateForGroup(1)
				So(err, ShouldBeNil)
				So(dcss, ShouldResemble, dgut.DCSs{
					{
						Dir:   projectA,
						Count: 2,
						Size:  21,
						Atime: expectedAtime,
						Mtime: expectedMtimeA,
						GIDs:  []uint32{1},
						UIDs:  []uint32{101},
						FTs:   expectedFTsBam,
					},
				})

				dcss, err = bd.CalculateForGroup(2)
				So(err, ShouldBeNil)
				So(dcss, ShouldResemble, dgut.DCSs{
					{
						Dir:   projectC1,
						Count: 1,
						Size:  40,
						Atime: expectedAtime,
						Mtime: expectedMtime,
						GIDs:  []uint32{2},
						UIDs:  []uint32{103},
						FTs:   expectedFTsBam,
					},
					{
						Dir:   projectB123,
						Count: 1,
						Size:  30,
						Atime: expectedAtime,
						Mtime: expectedMtime,
						GIDs:  []uint32{2},
						UIDs:  []uint32{102},
						FTs:   expectedFTsBam,
					},
					{
						Dir:   projectB125,
						Count: 1,
						Size:  20,
						Atime: expectedAtime,
						Mtime: expectedMtime,
						GIDs:  []uint32{2},
						UIDs:  []uint32{102},
						FTs:   expectedFTsBam,
					},
				})
			})

			Convey("of each user", func() { //nolint:dupl
				dcss, err := bd.CalculateForUser(101)
				So(err, ShouldBeNil)
				So(dcss, ShouldResemble, dgut.DCSs{
					{
						Dir:   projectA,
						Count: 2,
						Size:  21,
						Atime: expectedAtime,
						Mtime: expectedMtimeA,
						GIDs:  []uint32{1},
						UIDs:  []uint32{101},
						FTs:   expectedFTsBam,
					},
				})

				dcss, err = bd.CalculateForUser(102)
				So(err, ShouldBeNil)
				So(dcss, ShouldResemble, dgut.DCSs{
					{
						Dir:   projectB123,
						Count: 1,
						Size:  30,
						Atime: expectedAtime,
						Mtime: expectedMtime,
						GIDs:  []uint32{2},
						UIDs:  []uint32{102},
						FTs:   expectedFTsBam,
					},
					{
						Dir:   projectB125,
						Count: 1,
						Size:  20,
						Atime: expectedAtime,
						Mtime: expectedMtime,
						GIDs:  []uint32{2},
						UIDs:  []uint32{102},
						FTs:   expectedFTsBam,
					},
					{
						Dir:   user2,
						Count: 1,
						Size:  60,
						Atime: expectedAtime,
						Mtime: expectedMtime,
						GIDs:  []uint32{3},
						UIDs:  []uint32{102},
						FTs:   expectedFTsBam,
					},
				})
			})
		})

		Convey("With which you can store group and user summary info in a database", func() {
			yesterday := fixtimes.FixTime(time.Now().Add(-24 * time.Hour))
			err := bd.CreateDatabase(yesterday)
			So(err, ShouldBeNil)

			_, err = os.Stat(dbPath)
			So(err, ShouldBeNil)

			Convey("and then read the database", func() {
				bdr, err := NewReader(dbPath)
				So(err, ShouldBeNil)

				bdr.mountPoints = bd.mountPoints

				expectedMtime := fixtimes.FixTime(time.Unix(50, 0))
				expectedMtimeA := fixtimes.FixTime(time.Unix(100, 0))

				Convey("getting group and user usage info", func() {
					mainTable, err := bdr.GroupUsage()
					fixUsageTimes(mainTable)

					So(err, ShouldBeNil)
					So(len(mainTable), ShouldEqual, 5)
					So(mainTable, ShouldResemble, []*Usage{
						{GID: 1, BaseDir: projectA, UsageSize: 21, QuotaSize: 200,
							UsageInodes: 2, QuotaInodes: 20, Mtime: expectedMtimeA},
						{GID: 2, BaseDir: projectC1, UsageSize: 40, QuotaSize: 400,
							UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime},
						{GID: 2, BaseDir: projectB123, UsageSize: 30, QuotaSize: 400,
							UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime},
						{GID: 2, BaseDir: projectB125, UsageSize: 20, QuotaSize: 300,
							UsageInodes: 1, QuotaInodes: 30, Mtime: expectedMtime},
						{GID: 3, BaseDir: user2, UsageSize: 60, QuotaSize: 500,
							UsageInodes: 1, QuotaInodes: 50, Mtime: expectedMtime},
					})

					mainTable, err = bdr.UserUsage()
					fixUsageTimes(mainTable)

					So(err, ShouldBeNil)
					So(len(mainTable), ShouldEqual, 5)
					So(mainTable, ShouldResemble, []*Usage{
						{UID: 101, BaseDir: projectA, UsageSize: 21, UsageInodes: 2,
							Mtime: expectedMtimeA},
						{UID: 102, BaseDir: projectB123, UsageSize: 30, UsageInodes: 1,
							Mtime: expectedMtime},
						{UID: 102, BaseDir: projectB125, UsageSize: 20, UsageInodes: 1,
							Mtime: expectedMtime},
						{UID: 102, BaseDir: user2, UsageSize: 60, UsageInodes: 1,
							Mtime: expectedMtime},
						{UID: 103, BaseDir: projectC1, UsageSize: 40, UsageInodes: 1,
							Mtime: expectedMtime},
					})
				})

				Convey("getting group historical quota", func() {
					expectedAHistory := History{
						Date:        yesterday,
						UsageSize:   21,
						QuotaSize:   200,
						UsageInodes: 2,
						QuotaInodes: 20,
					}

					history, err := bdr.History(1, projectA)
					fixHistoryTimes(history)

					So(err, ShouldBeNil)
					So(len(history), ShouldEqual, 1)
					So(history, ShouldResemble, []History{expectedAHistory})

					history, err = bdr.History(1, filepath.Join(projectA, "newsub"))
					fixHistoryTimes(history)

					So(err, ShouldBeNil)
					So(len(history), ShouldEqual, 1)
					So(history, ShouldResemble, []History{expectedAHistory})

					history, err = bdr.History(2, projectB125)
					fixHistoryTimes(history)

					So(err, ShouldBeNil)
					So(len(history), ShouldEqual, 1)
					So(history, ShouldResemble, []History{
						{
							Date:        yesterday,
							UsageSize:   20,
							QuotaSize:   300,
							UsageInodes: 1,
							QuotaInodes: 30,
						},
					})

					dtrSize, dtrInode := DateQuotaFull(history)
					So(dtrSize, ShouldEqual, time.Time{})
					So(dtrInode, ShouldEqual, time.Time{})

					err = bdr.Close()
					So(err, ShouldBeNil)

					Convey("Then you can add and retrieve a new day's usage and quota", func() {
						_, files := testFiles()
						files[0].NumFiles = 2
						files = files[:len(files)-1]
						tree = createTestTreeDB(t, files)

						quotas.gids[1][0].quotaSize = 201
						quotas.gids[1][0].quotaInode = 21

						bd, err = NewCreator(dbPath, tree, quotas)
						So(err, ShouldBeNil)
						So(bd, ShouldNotBeNil)

						today := fixtimes.FixTime(time.Now())
						err := bd.CreateDatabase(today)
						So(err, ShouldBeNil)

						bdr, err = NewReader(dbPath)
						So(err, ShouldBeNil)

						bdr.mountPoints = bd.mountPoints

						mainTable, err := bdr.GroupUsage()
						fixUsageTimes(mainTable)

						So(err, ShouldBeNil)
						So(len(mainTable), ShouldEqual, 4)
						So(mainTable, ShouldResemble, []*Usage{
							{GID: 1, BaseDir: projectA, UsageSize: 31, QuotaSize: 201,
								UsageInodes: 3, QuotaInodes: 21, Mtime: expectedMtimeA},
							{GID: 2, BaseDir: projectC1, UsageSize: 40, QuotaSize: 400,
								UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime},
							{GID: 2, BaseDir: projectB123, UsageSize: 30, QuotaSize: 400,
								UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime},
							{GID: 2, BaseDir: projectB125, UsageSize: 20, QuotaSize: 300,
								UsageInodes: 1, QuotaInodes: 30, Mtime: expectedMtime},
						})

						history, err := bdr.History(1, projectA)
						fixHistoryTimes(history)

						So(err, ShouldBeNil)
						So(len(history), ShouldEqual, 2)
						So(history, ShouldResemble, []History{
							expectedAHistory,
							{
								Date:        today,
								UsageSize:   31,
								QuotaSize:   201,
								UsageInodes: 3,
								QuotaInodes: 21,
							},
						})

						expectedUntilSize := today.Add(secondsInDay * 17).Unix()
						expectedUntilInode := today.Add(secondsInDay * 18).Unix()

						dtrSize, dtrInode := DateQuotaFull(history)
						So(dtrSize.Unix(), ShouldBeBetween, expectedUntilSize-2, expectedUntilSize+2)
						So(dtrInode.Unix(), ShouldBeBetween, expectedUntilInode-2, expectedUntilInode+2)
					})
				})
			})
		})
	})
}

func fixUsageTimes(mt []*Usage) {
	for _, u := range mt {
		u.Mtime = fixtimes.FixTime(u.Mtime)
	}
}

func fixHistoryTimes(history []History) {
	for n := range history {
		history[n].Date = fixtimes.FixTime(history[n].Date)
	}
}
