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
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/v4/dgut"
	internaldata "github.com/wtsi-ssg/wrstat/v4/internal/data"
	internaldb "github.com/wtsi-ssg/wrstat/v4/internal/db"
	"github.com/wtsi-ssg/wrstat/v4/internal/fixtimes"
	"github.com/wtsi-ssg/wrstat/v4/summary"
	bolt "go.etcd.io/bbolt"
)

func TestBaseDirs(t *testing.T) { //nolint:gocognit
	csvPath := internaldata.CreateQuotasCSV(t, `1,/lustre/scratch125,4000000000,20
2,/lustre/scratch125,300,30
2,/lustre/scratch123,400,40
77777,/lustre/scratch125,500,50
1,/nfs/scratch125,4000000000,20
2,/nfs/scratch125,300,30
2,/nfs/scratch123,400,40
77777,/nfs/scratch125,500,50
`)

	Convey("Given a Tree and Quotas you can make a BaseDirs", t, func() {
		gid, uid, groupName, username, err := internaldata.RealGIDAndUID()
		So(err, ShouldBeNil)

		locDirs, files := internaldata.FakeFilesForDGUTDBForBasedirsTesting(gid, uid)

		const (
			halfGig = 1 << 29
			twoGig  = 1 << 31
			splits  = 4
			minDirs = 4
		)

		files[0].SizeOfEachFile = halfGig
		files[1].SizeOfEachFile = twoGig

		tree, err := internaldb.CreateDGUTDBFromFakeFiles(t, files)
		So(err, ShouldBeNil)

		projectA := locDirs[0]
		projectB125 := locDirs[1]
		projectB123 := locDirs[2]
		projectC1 := locDirs[3]
		user2 := locDirs[5]
		projectD := locDirs[6]

		quotas, err := ParseQuotas(csvPath)
		So(err, ShouldBeNil)

		dir := t.TempDir()
		dbPath := filepath.Join(dir, "basedir.db")

		bd, err := NewCreator(dbPath, splits, minDirs, tree, quotas)
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
						Size:  halfGig + twoGig,
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
						UIDs:  []uint32{88888},
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
						Size:  halfGig + twoGig,
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
						GIDs:  []uint32{77777},
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
				ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
				So(err, ShouldBeNil)

				bdr, err := NewReader(dbPath, ownersPath)
				So(err, ShouldBeNil)

				bdr.mountPoints = bd.mountPoints

				groupCache := &GroupCache{
					data: map[uint32]string{
						1: "group1",
						2: "group2",
					},
				}
				bdr.groupCache = groupCache

				bdr.userCache = &UserCache{
					data: map[uint32]string{
						101: "user101",
						102: "user102",
					},
				}

				expectedMtime := fixtimes.FixTime(time.Unix(50, 0))
				expectedMtimeA := fixtimes.FixTime(time.Unix(100, 0))

				Convey("getting group and user usage info", func() {
					mainTable, err := bdr.GroupUsage()
					fixUsageTimes(mainTable)

					So(err, ShouldBeNil)
					So(len(mainTable), ShouldEqual, 6)
					So(mainTable, ShouldResemble, []*Usage{
						{
							Name: "group1", GID: 1, UIDs: []uint32{101}, Owner: "Alan", BaseDir: projectA,
							UsageSize: halfGig + twoGig, QuotaSize: 4000000000, UsageInodes: 2,
							QuotaInodes: 20, Mtime: expectedMtimeA,
						},
						{
							Name: groupName, GID: uint32(gid), UIDs: []uint32{uint32(uid)}, BaseDir: projectD,
							UsageSize: 15, QuotaSize: 0, UsageInodes: 5, QuotaInodes: 0, Mtime: expectedMtime,
							DateNoSpace: yesterday, DateNoFiles: yesterday,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{88888}, Owner: "Barbara", BaseDir: projectC1,
							UsageSize: 40, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectB123,
							UsageSize: 30, QuotaSize: 400, UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime,
						},
						{
							Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectB125,
							UsageSize: 20, QuotaSize: 300, UsageInodes: 1, QuotaInodes: 30, Mtime: expectedMtime,
						},
						{
							Name: "77777", GID: 77777, UIDs: []uint32{102}, Owner: "", BaseDir: user2, UsageSize: 60,
							QuotaSize: 500, UsageInodes: 1, QuotaInodes: 50, Mtime: expectedMtime,
						},
					})

					mainTable, err = bdr.UserUsage()
					fixUsageTimes(mainTable)

					expectedMainTable := []*Usage{
						{
							Name: "user101", UID: 101, GIDs: []uint32{1}, BaseDir: projectA,
							UsageSize: halfGig + twoGig, UsageInodes: 2, Mtime: expectedMtimeA,
						},
						{
							Name: "user102", UID: 102, GIDs: []uint32{2}, BaseDir: projectB123, UsageSize: 30,
							UsageInodes: 1, Mtime: expectedMtime,
						},
						{
							Name: "user102", UID: 102, GIDs: []uint32{2}, BaseDir: projectB125, UsageSize: 20,
							UsageInodes: 1, Mtime: expectedMtime,
						},
						{
							Name: "user102", UID: 102, GIDs: []uint32{77777}, BaseDir: user2, UsageSize: 60,
							UsageInodes: 1, Mtime: expectedMtime,
						},
						{
							Name: "88888", UID: 88888, GIDs: []uint32{2}, BaseDir: projectC1, UsageSize: 40,
							UsageInodes: 1, Mtime: expectedMtime,
						},
						{
							Name: username, UID: uint32(uid), GIDs: []uint32{uint32(gid)}, BaseDir: projectD,
							UsageSize: 15, UsageInodes: 5, Mtime: expectedMtime,
						},
					}

					sort.Slice(expectedMainTable, func(i, j int) bool {
						iID := strconv.FormatUint(uint64(expectedMainTable[i].UID), 10)
						jID := strconv.FormatUint(uint64(expectedMainTable[j].UID), 10)

						return iID < jID
					})

					So(err, ShouldBeNil)
					So(len(mainTable), ShouldEqual, 6)
					So(mainTable, ShouldResemble, expectedMainTable)
				})

				Convey("getting group historical quota", func() {
					expectedAHistory := History{
						Date:        yesterday,
						UsageSize:   halfGig + twoGig,
						QuotaSize:   4000000000,
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
						_, files := internaldata.FakeFilesForDGUTDBForBasedirsTesting(gid, uid)
						files[0].NumFiles = 2
						files[0].SizeOfEachFile = halfGig
						files[1].SizeOfEachFile = twoGig

						files = files[:len(files)-1]
						tree, err = internaldb.CreateDGUTDBFromFakeFiles(t, files)
						So(err, ShouldBeNil)

						const fiveGig = 5 * (1 << 30)

						quotas.gids[1][0].quotaSize = fiveGig
						quotas.gids[1][0].quotaInode = 21

						mp := bd.mountPoints

						bd, err = NewCreator(dbPath, splits, minDirs, tree, quotas)
						So(err, ShouldBeNil)
						So(bd, ShouldNotBeNil)

						bd.mountPoints = mp

						today := fixtimes.FixTime(time.Now())
						err := bd.CreateDatabase(today)
						So(err, ShouldBeNil)

						bdr, err = NewReader(dbPath, ownersPath)
						So(err, ShouldBeNil)

						bdr.mountPoints = bd.mountPoints
						bdr.groupCache = groupCache

						mainTable, err := bdr.GroupUsage()
						fixUsageTimes(mainTable)

						dateNoSpace := today.Add(4 * 24 * time.Hour)
						So(mainTable[0].DateNoSpace, ShouldHappenOnOrBetween,
							dateNoSpace.Add(-2*time.Second), dateNoSpace.Add(2*time.Second))

						dateNoTime := today.Add(18 * 24 * time.Hour)
						So(mainTable[0].DateNoFiles, ShouldHappenOnOrBetween,
							dateNoTime.Add(-2*time.Second), dateNoTime.Add(2*time.Second))

						mainTable[0].DateNoSpace = time.Time{}
						mainTable[0].DateNoFiles = time.Time{}

						So(err, ShouldBeNil)
						So(len(mainTable), ShouldEqual, 6)
						So(mainTable, ShouldResemble, []*Usage{
							{
								Name: "group1", GID: 1, UIDs: []uint32{101}, Owner: "Alan", BaseDir: projectA,
								UsageSize: twoGig + halfGig*2, QuotaSize: fiveGig,
								UsageInodes: 3, QuotaInodes: 21, Mtime: expectedMtimeA,
							},
							{
								Name: groupName, GID: uint32(gid), UIDs: []uint32{uint32(uid)}, BaseDir: projectD,
								UsageSize: 10, QuotaSize: 0, UsageInodes: 4, QuotaInodes: 0, Mtime: expectedMtime,
								DateNoSpace: today, DateNoFiles: today,
							},
							{
								Name: "group2", GID: 2, UIDs: []uint32{88888}, Owner: "Barbara", BaseDir: projectC1,
								UsageSize: 40, QuotaSize: 400, UsageInodes: 1,
								QuotaInodes: 40, Mtime: expectedMtime,
							},
							{
								Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectB123,
								UsageSize: 30, QuotaSize: 400, UsageInodes: 1,
								QuotaInodes: 40, Mtime: expectedMtime,
							},
							{
								Name: "group2", GID: 2, UIDs: []uint32{102}, Owner: "Barbara", BaseDir: projectB125,
								UsageSize: 20, QuotaSize: 300, UsageInodes: 1,
								QuotaInodes: 30, Mtime: expectedMtime,
							},
							{
								Name: "77777", GID: 77777, UIDs: []uint32{102}, Owner: "", BaseDir: user2,
								UsageSize: 60, QuotaSize: 500, UsageInodes: 1,
								QuotaInodes: 50, Mtime: expectedMtime,
							},
						})

						history, err := bdr.History(1, projectA)
						fixHistoryTimes(history)

						So(err, ShouldBeNil)
						So(len(history), ShouldEqual, 2)
						So(history, ShouldResemble, []History{
							expectedAHistory,
							{
								Date:        today,
								UsageSize:   twoGig + halfGig*2,
								QuotaSize:   fiveGig,
								UsageInodes: 3,
								QuotaInodes: 21,
							},
						})

						expectedUntilSize := today.Add(secondsInDay * 4).Unix()
						expectedUntilInode := today.Add(secondsInDay * 18).Unix()

						dtrSize, dtrInode := DateQuotaFull(history)
						So(dtrSize.Unix(), ShouldBeBetween, expectedUntilSize-4, expectedUntilSize+4)
						So(dtrInode.Unix(), ShouldBeBetween, expectedUntilInode-4, expectedUntilInode+4)
					})
				})

				expectedProjectASubDirs := []*SubDir{
					{
						SubDir:    ".",
						NumFiles:  1,
						SizeFiles: halfGig,
						// actually expectedMtime, but we don't  have a way
						// of getting correct answer for "."
						LastModified: expectedMtimeA,
						FileUsage: map[summary.DirGUTFileType]uint64{
							summary.DGUTFileTypeBam: halfGig,
						},
					},
					{
						SubDir:       "sub",
						NumFiles:     1,
						SizeFiles:    twoGig,
						LastModified: expectedMtimeA,
						FileUsage: map[summary.DirGUTFileType]uint64{
							summary.DGUTFileTypeBam: twoGig,
						},
					},
				}

				Convey("getting subdir information for a group-basedir", func() {
					unknownProject, err := bdr.GroupSubDirs(1, "unknown")
					So(err, ShouldBeNil)
					So(unknownProject, ShouldBeNil)

					unknownGroup, err := bdr.GroupSubDirs(10, projectA)
					So(err, ShouldBeNil)
					So(unknownGroup, ShouldBeNil)

					subdirsA1, err := bdr.GroupSubDirs(1, projectA)
					So(err, ShouldBeNil)

					fixSubDirTimes(subdirsA1)
					So(subdirsA1, ShouldResemble, expectedProjectASubDirs)
				})

				Convey("getting subdir information for a user-basedir", func() {
					unknownProject, err := bdr.UserSubDirs(101, "unknown")
					So(err, ShouldBeNil)
					So(unknownProject, ShouldBeNil)

					unknownGroup, err := bdr.UserSubDirs(999, projectA)
					So(err, ShouldBeNil)
					So(unknownGroup, ShouldBeNil)

					subdirsA1, err := bdr.UserSubDirs(101, projectA)
					So(err, ShouldBeNil)

					fixSubDirTimes(subdirsA1)
					So(subdirsA1, ShouldResemble, expectedProjectASubDirs)

					subdirsB125, err := bdr.UserSubDirs(102, projectB125)
					So(err, ShouldBeNil)

					fixSubDirTimes(subdirsB125)
					So(subdirsB125, ShouldResemble, []*SubDir{
						{
							SubDir:       ".",
							NumFiles:     1,
							SizeFiles:    20,
							LastModified: expectedMtime,
							FileUsage: UsageBreakdownByType{
								summary.DGUTFileTypeBam: 20,
							},
						},
					})

					subdirsB123, err := bdr.UserSubDirs(102, projectB123)
					So(err, ShouldBeNil)

					fixSubDirTimes(subdirsB123)
					So(subdirsB123, ShouldResemble, []*SubDir{
						{
							SubDir:       ".",
							NumFiles:     1,
							SizeFiles:    30,
							LastModified: expectedMtime,
							FileUsage: UsageBreakdownByType{
								summary.DGUTFileTypeBam: 30,
							},
						},
					})

					subdirsD, err := bdr.UserSubDirs(uint32(uid), projectD)
					So(err, ShouldBeNil)

					fixSubDirTimes(subdirsD)
					So(subdirsD, ShouldResemble, []*SubDir{
						{
							SubDir:       "sub1",
							NumFiles:     3,
							SizeFiles:    6,
							LastModified: expectedMtime,
							FileUsage: UsageBreakdownByType{
								summary.DGUTFileTypeTemp: 1026,
								summary.DGUTFileTypeBam:  1,
								summary.DGUTFileTypeSam:  2,
								summary.DGUTFileTypeCram: 3,
							},
						},
						{
							SubDir:       "sub2",
							NumFiles:     2,
							SizeFiles:    9,
							LastModified: expectedMtime,
							FileUsage: UsageBreakdownByType{
								summary.DGUTFileTypePedBed: 9,
							},
						},
					})
				})

				joinWithNewLines := func(rows ...string) string {
					return strings.Join(rows, "\n") + "\n"
				}

				joinWithTabs := func(cols ...string) string {
					return strings.Join(cols, "\t")
				}

				daysSinceString := func(mtime time.Time) string {
					return strconv.FormatUint(daysSince(mtime), 10)
				}

				expectedDaysSince := daysSinceString(expectedMtime)

				Convey("getting weaver-like output for group base-dirs", func() {
					wbo, err := bdr.GroupUsageTable()
					So(err, ShouldBeNil)
					So(wbo, ShouldEqual, joinWithNewLines(
						joinWithTabs(
							"group1",
							"Alan",
							projectA,
							expectedDaysSince,
							"2684354560",
							"4000000000",
							"2",
							"20",
							quotaStatusOK,
						),
						joinWithTabs(
							groupName,
							"",
							projectD,
							expectedDaysSince,
							"15",
							"0",
							"5",
							"0",
							quotaStatusNotOK,
						),
						joinWithTabs(
							"group2",
							"Barbara",
							projectC1,
							expectedDaysSince,
							"40",
							"400",
							"1",
							"40",
							quotaStatusOK,
						),
						joinWithTabs(
							"group2",
							"Barbara",
							projectB123,
							expectedDaysSince,
							"30",
							"400",
							"1",
							"40",
							quotaStatusOK,
						),
						joinWithTabs(
							"group2",
							"Barbara",
							projectB125,
							expectedDaysSince,
							"20",
							"300",
							"1",
							"30",
							quotaStatusOK,
						),
						joinWithTabs(
							"77777",
							"",
							user2,
							expectedDaysSince,
							"60",
							"500",
							"1",
							"50",
							quotaStatusOK,
						),
					))
				})

				Convey("getting weaver-like output for user base-dirs", func() {
					wbo, err := bdr.UserUsageTable()
					So(err, ShouldBeNil)

					groupsToID := make(map[string]uint32, len(bdr.userCache.data))

					for uid, name := range bdr.userCache.data {
						groupsToID[name] = uid
					}

					rowsData := [][]string{
						{
							"user101",
							"",
							projectA,
							expectedDaysSince,
							"2684354560",
							"0",
							"2",
							"0",
							quotaStatusOK,
						},
						{
							"user102",
							"",
							projectB123,
							expectedDaysSince,
							"30",
							"0",
							"1",
							"0",
							quotaStatusOK,
						},
						{
							"user102",
							"",
							projectB125,
							expectedDaysSince,
							"20",
							"0",
							"1",
							"0",
							quotaStatusOK,
						},
						{
							"user102",
							"",
							user2,
							expectedDaysSince,
							"60",
							"0",
							"1",
							"0",
							quotaStatusOK,
						},
						{
							"88888",
							"",
							projectC1,
							expectedDaysSince,
							"40",
							"0",
							"1",
							"0",
							quotaStatusOK,
						},
						{
							username,
							"",
							projectD,
							expectedDaysSince,
							"15",
							"0",
							"5",
							"0",
							quotaStatusOK,
						},
					}

					sort.Slice(rowsData, func(i, j int) bool {
						iID := strconv.FormatUint(uint64(groupsToID[rowsData[i][0]]), 10)
						jID := strconv.FormatUint(uint64(groupsToID[rowsData[j][0]]), 10)

						return iID < jID
					})

					rows := make([]string, len(rowsData))
					for n, r := range rowsData {
						rows[n] = joinWithTabs(r...)
					}

					So(wbo, ShouldEqual, joinWithNewLines(rows...))
				})

				expectedProjectASubDirUsage := joinWithNewLines(
					joinWithTabs(
						projectA,
						".",
						"1",
						"536870912",
						expectedDaysSince,
						"bam: 0.50",
					),
					joinWithTabs(
						projectA,
						"sub",
						"1",
						"2147483648",
						expectedDaysSince,
						"bam: 2.00",
					),
				)

				Convey("getting weaver-like output for group sub-dirs", func() {
					unknown, err := bdr.GroupSubDirUsageTable(1, "unknown")
					So(err, ShouldBeNil)
					So(unknown, ShouldBeEmpty)

					badgroup, err := bdr.GroupSubDirUsageTable(999, projectA)
					So(err, ShouldBeNil)
					So(badgroup, ShouldBeEmpty)

					wso, err := bdr.GroupSubDirUsageTable(1, projectA)
					So(err, ShouldBeNil)
					So(wso, ShouldEqual, expectedProjectASubDirUsage)
				})

				Convey("getting weaver-like output for user sub-dirs", func() {
					unknown, err := bdr.UserSubDirUsageTable(1, "unknown")
					So(err, ShouldBeNil)
					So(unknown, ShouldBeEmpty)

					badgroup, err := bdr.UserSubDirUsageTable(999, projectA)
					So(err, ShouldBeNil)
					So(badgroup, ShouldBeEmpty)

					wso, err := bdr.UserSubDirUsageTable(101, projectA)
					So(err, ShouldBeNil)
					So(wso, ShouldEqual, expectedProjectASubDirUsage)
				})
			})

			Convey("and merge with another database", func() {
				_, newFiles := internaldata.FakeFilesForDGUTDBForBasedirsTesting(gid, uid)
				for i := range newFiles {
					newFiles[i].Path = "/nfs" + newFiles[i].Path[7:]
				}

				newTree, err := internaldb.CreateDGUTDBFromFakeFiles(t, newFiles)
				So(err, ShouldBeNil)

				newDBPath := filepath.Join(dir, "newdir.db")

				newBd, err := NewCreator(newDBPath, splits, minDirs, newTree, quotas)
				So(err, ShouldBeNil)
				So(bd, ShouldNotBeNil)

				newBd.mountPoints = mountPoints{
					"/nfs/scratch123/",
					"/nfs/scratch125/",
				}

				err = newBd.CreateDatabase(yesterday)
				So(err, ShouldBeNil)

				outputDBPath := filepath.Join(dir, "merged.db")

				err = MergeDBs(dbPath, newDBPath, outputDBPath)
				So(err, ShouldBeNil)

				db, err := openRODB(outputDBPath)

				So(err, ShouldBeNil)
				defer db.Close()

				countKeys := func(bucket string) (int, int) {
					lustreKeys, nfsKeys := 0, 0

					db.View(func(tx *bolt.Tx) error { //nolint:errcheck
						bucket := tx.Bucket([]byte(bucket))

						return bucket.ForEach(func(k, _ []byte) error {
							if strings.Contains(string(k), "/lustre/") {
								lustreKeys++
							}
							if strings.Contains(string(k), "/nfs/") {
								nfsKeys++
							}

							return nil
						})
					})

					return lustreKeys, nfsKeys
				}

				expectedKeys := 6

				lustreKeys, nfsKeys := countKeys(groupUsageBucket)
				So(lustreKeys, ShouldEqual, expectedKeys)
				So(nfsKeys, ShouldEqual, expectedKeys)

				lustreKeys, nfsKeys = countKeys(groupHistoricalBucket)
				So(lustreKeys, ShouldEqual, 5)
				So(nfsKeys, ShouldEqual, 5)

				lustreKeys, nfsKeys = countKeys(groupSubDirsBucket)
				So(lustreKeys, ShouldEqual, expectedKeys)
				So(nfsKeys, ShouldEqual, expectedKeys)

				lustreKeys, nfsKeys = countKeys(userUsageBucket)
				So(lustreKeys, ShouldEqual, expectedKeys)
				So(nfsKeys, ShouldEqual, expectedKeys)

				lustreKeys, nfsKeys = countKeys(userSubDirsBucket)
				So(lustreKeys, ShouldEqual, expectedKeys)
				So(nfsKeys, ShouldEqual, expectedKeys)
			})
		})
	})
}

func TestOwners(t *testing.T) {
	Convey("Given an owners tsv you can parse it", t, func() {
		ownersPath, err := internaldata.CreateOwnersCSV(t, internaldata.ExampleOwnersCSV)
		So(err, ShouldBeNil)

		owners, err := parseOwners(ownersPath)
		So(err, ShouldBeNil)
		So(owners, ShouldResemble, map[uint32]string{
			1: "Alan",
			2: "Barbara",
			4: "Dellilah",
		})
	})
}

func TestCaches(t *testing.T) {
	Convey("Given a GroupCache, accessing it in multiple threads should be safe.", t, func() {
		var wg sync.WaitGroup

		g := NewGroupCache()

		wg.Add(2)

		go func() {
			g.GroupName(0)
			wg.Done()
		}()

		go func() {
			g.GroupName(0)
			wg.Done()
		}()

		wg.Wait()
	})

	Convey("Given a UserCache, accessing it in multiple threads should be safe.", t, func() {
		var wg sync.WaitGroup

		u := NewUserCache()

		wg.Add(2)

		go func() {
			u.UserName(0)
			wg.Done()
		}()

		go func() {
			u.UserName(0)
			wg.Done()
		}()

		wg.Wait()
	})
}

func fixUsageTimes(mt []*Usage) {
	for _, u := range mt {
		u.Mtime = fixtimes.FixTime(u.Mtime)

		if !u.DateNoSpace.IsZero() {
			u.DateNoSpace = fixtimes.FixTime(u.DateNoSpace)
			u.DateNoFiles = fixtimes.FixTime(u.DateNoFiles)
		}
	}
}

func fixHistoryTimes(history []History) {
	for n := range history {
		history[n].Date = fixtimes.FixTime(history[n].Date)
	}
}

func fixSubDirTimes(sds []*SubDir) {
	for n := range sds {
		sds[n].LastModified = fixtimes.FixTime(sds[n].LastModified)
	}
}
