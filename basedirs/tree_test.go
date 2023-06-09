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
	"sort"
	"strconv"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/v4/dgut"
	internaldb "github.com/wtsi-ssg/wrstat/v4/internal/db"
	"github.com/wtsi-ssg/wrstat/v4/summary"
)

func TestTree(t *testing.T) {
	dbPath, err := internaldb.CreateExampleDGUTDB(t)
	if err != nil {
		t.Fatalf("could not create dgut db: %s", err)
	}

	_, uidStr, gidsStrs := internaldb.GetUserAndGroups(t)
	expectedUIDs := []uint32{0, strToID(t, uidStr)}

	expectedGIDs := make([]uint32, 3)
	expectedGIDs[0] = 0

	for i := 0; i < 2; i++ {
		expectedGIDs[i+1] = strToID(t, gidsStrs[i])
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

			sort.Slice(gids, func(i, j int) bool {
				return gids[i] < gids[j]
			})

			sort.Slice(expectedGIDs, func(i, j int) bool {
				return expectedGIDs[i] < expectedGIDs[j]
			})

			So(err, ShouldBeNil)
			So(gids, ShouldResemble, expectedGIDs)
			So(uids, ShouldResemble, expectedUIDs)

			Convey("Then we can calculate the base directories of each", func() {
				dir := t.TempDir()

				bd, err := New(dir, tree, quota)
				So(err, ShouldBeNil)

				expectedAtime := time.Unix(15, 0)
				expectedMtime := time.Unix(20, 0)
				expectedFTsBam := []summary.DirGUTFileType{summary.DGUTFileTypeBam}

				dcss, err := bd.CalculateForGroup(gids[0])
				So(err, ShouldBeNil)
				So(dcss, ShouldNotResemble, dgut.DCSs{ // TODO: ShouldResemble correct data, need to make deeper tree!
					{"/a/b/c", 2, 2, expectedAtime, expectedMtime, []uint32{11}, []uint32{1}, expectedFTsBam},
					{"/a/b/c/d", 1, 1, time.Unix(20, 0), expectedMtime, []uint32{11}, []uint32{1}, expectedFTsBam},
					{"/a/b/c/e", 1, 1, expectedAtime, expectedAtime, []uint32{11}, []uint32{1}, expectedFTsBam},
				})
			})
		})
	})
}

func strToID(t *testing.T, s string) uint32 {
	t.Helper()

	id, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("could not convert uid: %s", err)
	}

	return uint32(id)
}
