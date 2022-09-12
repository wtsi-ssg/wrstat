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
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const testFilePerms = 0644

func TestQuotas(t *testing.T) {
	csvPath := makeQuotasCSV(t, `1,/disk/1,10
1,/disk/2,11
2,/disk/1,12
`)

	Convey("Given a valid quotas csv file you can parse it", t, func() {
		quota, err := ParseQuotas(csvPath)
		So(err, ShouldBeNil)
		So(quota, ShouldNotBeNil)

		Convey("Then get the quota of a gid and disk", func() {
			s := quota.Get(1, "/disk/1/sub")
			So(s, ShouldEqual, 10)

			s = quota.Get(1, "/disk/2/sub")
			So(s, ShouldEqual, 11)

			s = quota.Get(2, "/disk/1/sub")
			So(s, ShouldEqual, 12)
		})

		Convey("Invalid gids and disks return 0 quota", func() {
			s := quota.Get(3, "/disk/1/sub")
			So(s, ShouldEqual, 0)

			s = quota.Get(2, "/disk/2/sub")
			So(s, ShouldEqual, 0)
		})
	})

	Convey("Invalid quotas csv files can't be parsed", t, func() {
		csvPath = makeQuotasCSV(t, `1,/disk/1`)
		_, err := ParseQuotas(csvPath)
		So(err, ShouldNotBeNil)
		So(err, ShouldEqual, errBadQuotaCSVFile)

		csvPath = makeQuotasCSV(t, `g,/disk/1,10`)
		_, err = ParseQuotas(csvPath)
		So(err, ShouldNotBeNil)

		csvPath = makeQuotasCSV(t, `1,/disk/1,s`)
		_, err = ParseQuotas(csvPath)
		So(err, ShouldNotBeNil)

		_, err = ParseQuotas("/foo")
		So(err, ShouldNotBeNil)
	})
}

// makeQuotasCSV creates a quotas csv file in a temp directory. Returns its
// path.
func makeQuotasCSV(t *testing.T, csv string) string {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "quotas.csv")

	if err := os.WriteFile(path, []byte(csv), testFilePerms); err != nil {
		t.Fatalf("could not write test csv file: %s", err)
	}

	return path
}
