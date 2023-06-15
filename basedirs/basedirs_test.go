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
	"io"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/v4/dgut"
	internaldata "github.com/wtsi-ssg/wrstat/v4/internal/data"
	"github.com/wtsi-ssg/wrstat/v4/internal/fixtimes"
	"github.com/wtsi-ssg/wrstat/v4/summary"
)

func TestBaseDirs(t *testing.T) { //nolint:gocognit
	csvPath := makeQuotasCSV(t, `1,/lustre/scratch125,4000000000,20
2,/lustre/scratch125,300,30
2,/lustre/scratch123,400,40
3,/lustre/scratch125,500,50
`)

	Convey("Given a Tree and Quotas you can make a BaseDirs", t, func() {
		locDirs, files := testFiles()

		const (
			halfGig = 1 << 29
			twoGig  = 1 << 31
		)

		files[0].SizeOfEachFile = halfGig
		files[1].SizeOfEachFile = twoGig

		projectD := filepath.Join("/", "lustre", "scratch125", "humgen", "projects", "D")
		projectDSub1 := filepath.Join(projectD, "sub1")
		projectDSub2 := filepath.Join(projectD, "sub2")

		u, err := user.Current()
		So(err, ShouldBeNil)

		username := u.Username

		uid64, err := strconv.ParseUint(u.Uid, 10, 64)
		So(err, ShouldBeNil)

		groups, err := u.GroupIds()
		So(err, ShouldBeNil)
		So(len(groups), ShouldBeGreaterThan, 0)

		gid64, err := strconv.ParseUint(groups[0], 10, 64)
		So(err, ShouldBeNil)

		group, err := user.LookupGroupId(groups[0])
		So(err, ShouldBeNil)

		uid := int(uid64)
		gid := int(gid64)

		files = append(files,
			internaldata.TestFile{
				Path:           filepath.Join(projectDSub1, "a.bam"),
				NumFiles:       1,
				SizeOfEachFile: 1,
				GID:            gid,
				UID:            uid,
				ATime:          50,
				MTime:          50,
			},
			internaldata.TestFile{
				Path:           filepath.Join(projectDSub1, "temp", "a.sam"),
				NumFiles:       1,
				SizeOfEachFile: 2,
				GID:            gid,
				UID:            uid,
				ATime:          50,
				MTime:          50,
			},
			internaldata.TestFile{
				Path:           filepath.Join(projectDSub1, "a.cram"),
				NumFiles:       1,
				SizeOfEachFile: 3,
				GID:            gid,
				UID:            uid,
				ATime:          50,
				MTime:          50,
			},
			internaldata.TestFile{
				Path:           filepath.Join(projectDSub2, "a.bed"),
				NumFiles:       1,
				SizeOfEachFile: 4,
				GID:            gid,
				UID:            uid,
				ATime:          50,
				MTime:          50,
			},
			internaldata.TestFile{
				Path:           filepath.Join(projectDSub2, "b.bed"),
				NumFiles:       1,
				SizeOfEachFile: 5,
				GID:            gid,
				UID:            uid,
				ATime:          50,
				MTime:          50,
			},
		)

		tree := createTestTreeDB(t, files)
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
				ownersPath := createOwnersFile(dir)

				bdr, err := NewReader(dbPath, ownersPath)
				So(err, ShouldBeNil)

				bdr.mountPoints = bd.mountPoints

				expectedMtime := fixtimes.FixTime(time.Unix(50, 0))
				expectedMtimeA := fixtimes.FixTime(time.Unix(100, 0))

				Convey("getting group and user usage info", func() {
					mainTable, err := bdr.GroupUsage()
					fixUsageTimes(mainTable)

					So(err, ShouldBeNil)
					So(len(mainTable), ShouldEqual, 6)
					So(mainTable, ShouldResemble, []*Usage{
						{GID: 1, Owner: "Alan", BaseDir: projectA, UsageSize: halfGig + twoGig, QuotaSize: 4000000000,
							UsageInodes: 2, QuotaInodes: 20, Mtime: expectedMtimeA},
						{GID: uint32(gid), BaseDir: projectD, UsageSize: 15, QuotaSize: 0,
							UsageInodes: 5, QuotaInodes: 0, Mtime: expectedMtime},
						{GID: 2, Owner: "Barbara", BaseDir: projectC1, UsageSize: 40, QuotaSize: 400,
							UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime},
						{GID: 2, Owner: "Barbara", BaseDir: projectB123, UsageSize: 30, QuotaSize: 400,
							UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime},
						{GID: 2, Owner: "Barbara", BaseDir: projectB125, UsageSize: 20, QuotaSize: 300,
							UsageInodes: 1, QuotaInodes: 30, Mtime: expectedMtime},
						{GID: 3, Owner: "Charles", BaseDir: user2, UsageSize: 60, QuotaSize: 500,
							UsageInodes: 1, QuotaInodes: 50, Mtime: expectedMtime},
					})

					mainTable, err = bdr.UserUsage()
					fixUsageTimes(mainTable)

					expectedMainTable := []*Usage{
						{UID: 101, BaseDir: projectA, UsageSize: halfGig + twoGig, UsageInodes: 2,
							Mtime: expectedMtimeA},
						{UID: 102, BaseDir: projectB123, UsageSize: 30, UsageInodes: 1,
							Mtime: expectedMtime},
						{UID: 102, BaseDir: projectB125, UsageSize: 20, UsageInodes: 1,
							Mtime: expectedMtime},
						{UID: 102, BaseDir: user2, UsageSize: 60, UsageInodes: 1,
							Mtime: expectedMtime},
						{UID: 103, BaseDir: projectC1, UsageSize: 40, UsageInodes: 1,
							Mtime: expectedMtime},
						{UID: uint32(uid), BaseDir: projectD, UsageSize: 15, UsageInodes: 5,
							Mtime: expectedMtime},
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
						_, files := testFiles()
						files[0].NumFiles = 2
						files[0].SizeOfEachFile = halfGig
						files[1].SizeOfEachFile = twoGig

						files = files[:len(files)-1]
						tree = createTestTreeDB(t, files)

						const fiveGig = 5 * (1 << 30)

						quotas.gids[1][0].quotaSize = fiveGig
						quotas.gids[1][0].quotaInode = 21

						mp := bd.mountPoints

						bd, err = NewCreator(dbPath, tree, quotas)
						So(err, ShouldBeNil)
						So(bd, ShouldNotBeNil)

						bd.mountPoints = mp

						today := fixtimes.FixTime(time.Now())
						err := bd.CreateDatabase(today)
						So(err, ShouldBeNil)

						bdr, err = NewReader(dbPath, ownersPath)
						So(err, ShouldBeNil)

						bdr.mountPoints = bd.mountPoints

						mainTable, err := bdr.GroupUsage()
						fixUsageTimes(mainTable)

						So(err, ShouldBeNil)
						So(len(mainTable), ShouldEqual, 4)
						So(mainTable, ShouldResemble, []*Usage{
							{GID: 1, Owner: "Alan", BaseDir: projectA, UsageSize: twoGig + halfGig*2, QuotaSize: fiveGig,
								UsageInodes: 3, QuotaInodes: 21, Mtime: expectedMtimeA},
							{GID: 2, Owner: "Barbara", BaseDir: projectC1, UsageSize: 40, QuotaSize: 400,
								UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime},
							{GID: 2, Owner: "Barbara", BaseDir: projectB123, UsageSize: 30, QuotaSize: 400,
								UsageInodes: 1, QuotaInodes: 40, Mtime: expectedMtime},
							{GID: 2, Owner: "Barbara", BaseDir: projectB125, UsageSize: 20, QuotaSize: 300,
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
								UsageSize:   twoGig + halfGig*2,
								QuotaSize:   fiveGig,
								UsageInodes: 3,
								QuotaInodes: 21,
							},
						})

						expectedUntilSize := today.Add(secondsInDay * 4).Unix()
						expectedUntilInode := today.Add(secondsInDay * 18).Unix()

						dtrSize, dtrInode := DateQuotaFull(history)
						So(dtrSize.Unix(), ShouldEqual, expectedUntilSize)
						So(dtrInode.Unix(), ShouldBeBetween, expectedUntilInode-2, expectedUntilInode+2)
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
					bdr.groupCache = GroupCache{
						1: "A",
						2: "2",
						3: "3",
					}

					wbo, err := bdr.GroupUsageTable()
					So(err, ShouldBeNil)
					So(wbo, ShouldEqual, joinWithNewLines(
						joinWithTabs(
							"2684354560",
							"4000000000",
							expectedDaysSince,
							projectA,
							quotaStatusOK,
							"Alan",
							"A",
						),
						joinWithTabs(
							"15",
							"0",
							expectedDaysSince,
							projectD,
							quotaStatusNotOK,
							"",
							group.Name,
						),
						joinWithTabs(
							"40",
							"400",
							expectedDaysSince,
							projectC1,
							quotaStatusOK,
							"Barbara",
							"2",
						),
						joinWithTabs(
							"30",
							"400",
							expectedDaysSince,
							projectB123,
							quotaStatusOK,
							"Barbara",
							"2",
						),
						joinWithTabs(
							"20",
							"300",
							expectedDaysSince,
							projectB125,
							quotaStatusOK,
							"Barbara",
							"2",
						),
						joinWithTabs(
							"60",
							"500",
							expectedDaysSince,
							user2,
							quotaStatusOK,
							"Charles",
							"3",
						),
					))
				})

				Convey("getting weaver-like output for user base-dirs", func() {
					bdr.userCache = UserCache{
						101: "A",
						102: "2",
						103: "3",
					}

					wbo, err := bdr.UserUsageTable()
					So(err, ShouldBeNil)

					groupsToID := make(map[string]uint32, len(bdr.userCache))

					for uid, name := range bdr.userCache {
						groupsToID[name] = uid
					}

					rowsData := [][]string{
						{
							"2684354560",
							"0",
							expectedDaysSince,
							projectA,
							quotaStatusOK,
							"",
							"A",
						},
						{
							"30",
							"0",
							expectedDaysSince,
							projectB123,
							quotaStatusOK,
							"",
							"2",
						},
						{
							"20",
							"0",
							expectedDaysSince,
							projectB125,
							quotaStatusOK,
							"",
							"2",
						},
						{
							"60",
							"0",
							expectedDaysSince,
							user2,
							quotaStatusOK,
							"",
							"2",
						},
						{
							"40",
							"0",
							expectedDaysSince,
							projectC1,
							quotaStatusOK,
							"",
							"3",
						},
						{
							"15",
							"0",
							expectedDaysSince,
							projectD,
							quotaStatusOK,
							"",
							username,
						},
					}

					sort.Slice(rowsData, func(i, j int) bool {
						iID := strconv.FormatUint(uint64(groupsToID[rowsData[i][6]]), 10)
						jID := strconv.FormatUint(uint64(groupsToID[rowsData[j][6]]), 10)

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
		})
	})
}

func TestOwners(t *testing.T) {
	Convey("Given an owners tsv you can parse it", t, func() {
		dir := t.TempDir()

		ownersPath := createOwnersFile(dir)

		owners, err := parseOwners(ownersPath)
		So(err, ShouldBeNil)
		So(owners, ShouldResemble, map[uint32]string{
			1: "Alan",
			2: "Barbara",
			3: "Charles",
			4: "Dellilah",
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

func fixSubDirTimes(sds []*SubDir) {
	for n := range sds {
		sds[n].LastModified = fixtimes.FixTime(sds[n].LastModified)
	}
}

func createOwnersFile(dir string) string {
	ownersPath := filepath.Join(dir, "owners.csv")

	writeFile(ownersPath, `1,Alan
2,Barbara
3,Charles
4,Dellilah`)

	return ownersPath
}

func writeFile(path, contents string) {
	f, err := os.Create(path)
	So(err, ShouldBeNil)

	_, err = io.WriteString(f, contents)
	So(err, ShouldBeNil)

	err = f.Close()
	So(err, ShouldBeNil)
}