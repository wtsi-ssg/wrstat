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

package basedirs

import (
	"path/filepath"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/v4/dgut"
	internaldata "github.com/wtsi-ssg/wrstat/v4/internal/data"
	internaldb "github.com/wtsi-ssg/wrstat/v4/internal/db"
	"github.com/wtsi-ssg/wrstat/v4/summary"
)

func TestTree(t *testing.T) {
	projectA := filepath.Join("/", "lustre", "scratch125", "humgen", "projects", "A")
	projectB125 := filepath.Join("/", "lustre", "scratch125", "humgen", "projects", "B")
	projectB123 := filepath.Join("/", "lustre", "scratch123", "hgi", "mdt1", "projects", "B")
	projectC1 := filepath.Join("/", "lustre", "scratch123", "hgi", "m0")
	projectC2 := filepath.Join("/", "lustre", "scratch123", "hgi", "mdt0")
	files := []internaldata.TestFile{
		{
			Path:           filepath.Join(projectA, "a.bam"),
			NumFiles:       1,
			SizeOfEachFile: 10,
			GID:            1,
			UID:            101,
			ATime:          50,
			MTime:          50,
		},
		{
			Path:           filepath.Join(projectA, "sub", "a.bam"),
			NumFiles:       1,
			SizeOfEachFile: 11,
			GID:            1,
			UID:            101,
			ATime:          50,
			MTime:          50,
		},
		{
			Path:           filepath.Join(projectB125, "b.bam"),
			NumFiles:       1,
			SizeOfEachFile: 20,
			GID:            2,
			UID:            102,
			ATime:          50,
			MTime:          50,
		},
		{
			Path:           filepath.Join(projectB123, "b.bam"),
			NumFiles:       1,
			SizeOfEachFile: 30,
			GID:            2,
			UID:            102,
			ATime:          50,
			MTime:          50,
		},
		{
			Path:           filepath.Join(projectC1, "c.bam"),
			NumFiles:       1,
			SizeOfEachFile: 40,
			GID:            2,
			UID:            102,
			ATime:          50,
			MTime:          50,
		},
		{
			Path:           filepath.Join(projectC2, "c.bam"),
			NumFiles:       1,
			SizeOfEachFile: 40,
			GID:            2,
			UID:            102,
			ATime:          50,
			MTime:          50,
		},
	}

	dgutData := internaldata.TestDGUTData(t, files)

	dbPath, err := internaldb.CreateCustomDB(t, dgutData)
	if err != nil {
		t.Fatalf("could not create dgut db: %s", err)
	}

	Convey("Given a Tree and quota", t, func() {
		tree, err := dgut.NewTree(dbPath)
		So(err, ShouldBeNil)
		So(tree, ShouldNotBeNil)

		csvPath := makeQuotasCSV(t, exampleQuotaCSV)
		quota, err := ParseQuotas(csvPath)
		So(err, ShouldBeNil)

		Convey("You can get all the gids and uids in it", func() {
			gids, uids, err := getAllGIDsandUIDsInTree(tree)
			expectedGIDs := []uint32{1, 2}
			expectedUIDs := []uint32{101, 102}
			So(err, ShouldBeNil)
			So(gids, ShouldResemble, expectedGIDs)
			So(uids, ShouldResemble, expectedUIDs)

			Convey("Then we can calculate the base directories of each", func() {
				dir := t.TempDir()

				bd, err := New(dir, tree, quota)
				So(err, ShouldBeNil)

				expectedAtime := time.Unix(50, 0)
				expectedMtime := time.Unix(50, 0)
				expectedFTsBam := []summary.DirGUTFileType{summary.DGUTFileTypeBam}

				dcss, err := bd.CalculateForGroup(1)
				So(err, ShouldBeNil)
				So(dcss, ShouldResemble, dgut.DCSs{
					{
						Dir:   projectA,
						Count: 2,
						Size:  21,
						Atime: expectedAtime,
						Mtime: expectedMtime,
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
						UIDs:  []uint32{102},
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
		})
	})
}
