/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Authors:
 *	- Sendu Bala <sb10@sanger.ac.uk>
 *	- Michael Grace <mg38@sanger.ac.uk>
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

// package internal provides some test-related functions needed by multiple
// other packages.

package internal

import (
	"os"
	"os/user"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wtsi-ssg/wrstat/v4/dgut"
)

const DirPerms = 0755
const ExampleDgutDirParentSuffix = "dgut.dbs"

// GetUserAndGroups returns the current users username, uid and gids.
func GetUserAndGroups(t *testing.T) (string, string, []string) {
	t.Helper()

	uu, err := user.Current()
	if err != nil {
		t.Logf("getting current user failed: %s", err.Error())

		return "", "", nil
	}

	gids, err := uu.GroupIds()
	if err != nil {
		t.Logf("getting group ids failed: %s", err.Error())

		return "", "", nil
	}

	return uu.Username, uu.Uid, gids
}

// CreateExampleDGUTDB creates a temporary dgut.db from some example data that
// uses your uid and 2 of your gids, and returns the path to the database
// directory. For use when testing something that needs a Tree.
func CreateExampleDGUTDB(t *testing.T) (string, error) {
	_, uid, gids := GetUserAndGroups(t)
	if len(gids) < 2 {
		gids = append(gids, "0")
	}

	return CreateExampleDB(t, uid, gids[0], gids[1])
}

// CreateExampleDB creates a temporary dgut.db from some example data that uses
// the given uid and gids, and returns the path to the database directory.
func CreateExampleDB(t *testing.T, uid, gidA, gidB string) (string, error) {
	t.Helper()

	dir, err := createExampleDgutDir(t)
	if err != nil {
		return dir, err
	}

	dgutData := exampleDGUTData(uid, gidA, gidB)
	data := strings.NewReader(dgutData)
	db := dgut.NewDB(dir)

	err = db.Store(data, 20)

	return dir, err
}

// createExampleDgutDir creates a temp directory structure to hold dgut db files
// in the same way that 'wrstat tidy' organises them.
func createExampleDgutDir(t *testing.T) (string, error) {
	t.Helper()

	tdir := t.TempDir()
	dir := filepath.Join(tdir, "orig."+ExampleDgutDirParentSuffix, "0")
	err := os.MkdirAll(dir, DirPerms)

	return dir, err
}

// exampleDGUTData is some example DGUT data that uses the given uid and gids,
// along with root's uid.
func exampleDGUTData(uid, gidA, gidB string) string {
	data := `/	x	z	7	3	30	50	60
/	x	z	6	2	10	50	75
/	x	z	1	1	5	50	80
/	x	0	7	4	40	50	75
/	y	0	7	5	5	50	90
/	0	0	7	1	1	50	50
/a	x	z	7	3	30	50	60
/a	x	z	6	2	10	50	75
/a	x	z	1	1	5	50	80
/a	x	0	7	4	40	50	75
/a	y	0	7	5	5	50	90
/a	0	0	7	1	1	50	50
/a/b	x	z	7	3	30	50	60
/a/b	x	z	6	2	10	50	75
/a/b	x	z	1	1	5	50	80
/a/b	x	0	7	4	40	50	75
/a/b/d	x	z	7	3	30	50	60
/a/b/d	x	0	7	4	40	50	75
/a/b/d/f	x	z	7	1	10	75	50
/a/b/d/g	x	z	7	2	20	50	60
/a/b/d/g	x	0	7	4	40	50	75
/a/b/e	x	z	6	2	10	50	75
/a/b/e	x	z	1	1	5	50	80
/a/b/e/h	x	z	6	2	10	50	75
/a/b/e/h	x	z	1	1	5	50	80
/a/b/e/h/tmp	x	z	6	1	5	50	75
/a/b/e/h/tmp	x	z	1	1	5	50	80
/a/c	y	0	7	5	5	50	90
/a/c/d	y	0	7	5	5	50	90
`

	data = strings.ReplaceAll(data, "x", gidA)
	data = strings.ReplaceAll(data, "y", gidB)
	data = strings.ReplaceAll(data, "z", uid)

	return data
}
