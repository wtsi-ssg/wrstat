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
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/v4/dgut"
	internaldata "github.com/wtsi-ssg/wrstat/v4/internal/data"
	internaldb "github.com/wtsi-ssg/wrstat/v4/internal/db"
)

func TestTree(t *testing.T) {
	Convey("Given a Tree", t, func() {
		tree, _ := baseDirsTree(t)

		Convey("You can get all the gids and uids in it", func() {
			gids, uids, err := getAllGIDsandUIDsInTree(tree)
			expectedGIDs := []uint32{1, 2, 77777}
			expectedUIDs := []uint32{101, 102, 88888}
			So(err, ShouldBeNil)
			So(gids, ShouldResemble, expectedGIDs)
			So(uids, ShouldResemble, expectedUIDs)
		})
	})
}

// baseDirsTree makes a tree database with data useful for testing basedirs,
// and returns it along with a slice of directories where the data is.
func baseDirsTree(t *testing.T) (*dgut.Tree, []string) {
	t.Helper()

	dirs, files := testFiles()

	return createTestTreeDB(t, files), dirs
}

func testFiles() ([]string, []internaldata.TestFile) {
	projectA := filepath.Join("/", "lustre", "scratch125", "humgen", "projects", "A")
	projectB125 := filepath.Join("/", "lustre", "scratch125", "humgen", "projects", "B")
	projectB123 := filepath.Join("/", "lustre", "scratch123", "hgi", "mdt1", "projects", "B")
	projectC1 := filepath.Join("/", "lustre", "scratch123", "hgi", "m0")
	projectC2 := filepath.Join("/", "lustre", "scratch123", "hgi", "mdt0")
	user2 := filepath.Join("/", "lustre", "scratch125", "humgen", "teams", "102")
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
			MTime:          100,
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
			UID:            88888,
			ATime:          50,
			MTime:          50,
		},
		{
			Path:           filepath.Join(projectC2, "c.bam"),
			NumFiles:       1,
			SizeOfEachFile: 40,
			GID:            2,
			UID:            88888,
			ATime:          50,
			MTime:          50,
		},
		{
			Path:           filepath.Join(user2, "d.bam"),
			NumFiles:       1,
			SizeOfEachFile: 60,
			GID:            77777,
			UID:            102,
			ATime:          50,
			MTime:          50,
		},
	}

	return []string{projectA, projectB125, projectB123, projectC1, projectC2, user2}, files
}

func createTestTreeDB(t *testing.T, files []internaldata.TestFile) *dgut.Tree {
	t.Helper()

	dgutData := internaldata.TestDGUTData(t, files)

	dbPath, err := internaldb.CreateCustomDB(t, dgutData)
	if err != nil {
		t.Fatalf("could not create dgut db: %s", err)
	}

	tree, err := dgut.NewTree(dbPath)
	So(err, ShouldBeNil)
	So(tree, ShouldNotBeNil)

	return tree
}
