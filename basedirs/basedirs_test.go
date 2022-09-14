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

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/dgut"
	"github.com/wtsi-ssg/wrstat/internal"
)

func TestBaseDirs(t *testing.T) {
	dbPath, err := internal.CreateExampleDGUTDB(t)
	if err != nil {
		t.Fatalf("could not create dgut db: %s", err)
	}

	csvPath := makeQuotasCSV(t, `1,/disk/1,10
1,/disk/2,11
2,/disk/1,12
`)

	Convey("Given a Tree and Quotas you can make a BaseDirs", t, func() {
		tree, err := dgut.NewTree(dbPath)
		So(err, ShouldBeNil)
		So(tree, ShouldNotBeNil)

		quotas, err := ParseQuotas(csvPath)
		So(err, ShouldBeNil)

		dir := t.TempDir()
		dbDir := filepath.Join(dir, "basedir.db")

		bd, err := New(dbDir, tree, quotas)
		So(err, ShouldBeNil)
		So(bd, ShouldNotBeNil)
	})
}
