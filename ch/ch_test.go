/*******************************************************************************
 * Copyright (c) 2021 Genome Research Ltd.
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

package ch

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/inconshreveable/log15"
	. "github.com/smartystreets/goconvey/convey"
)

const longBasename = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
	"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
	"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" +
	"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

func TestCh(t *testing.T) {
	primaryGID, otherGIDs := getGIDs(t)

	if len(otherGIDs) == 0 {
		SkipConvey("Can't test Ch since you don't belong to multiple groups", t, func() {})

		return
	}

	otherGID := otherGIDs[0]
	unchangedGIDs := []int{primaryGID, otherGID, primaryGID, primaryGID}
	primaryName := testGroupName(t, primaryGID)
	otherName := testGroupName(t, otherGID)

	Convey("groupName seems to do something reasonable", t, func() {
		name, err := groupName(primaryGID)
		So(err, ShouldBeNil)
		So(name, ShouldNotBeBlank)

		name, err = groupName(-1)
		So(err, ShouldNotBeNil)
		So(name, ShouldBeBlank)
	})

	Convey("Given a logger and test files", t, func() {
		buff, l := newLogger()

		dir, paths, infos := createTestFiles(t, primaryGID, otherGID)

		Convey("Do does nothing if no changes need to be made", func() {
			rs := NewRulesStore()
			ch := New(rs, l)
			So(ch, ShouldNotBeNil)

			for i, path := range paths {
				err := ch.Do(path, infos[i])
				So(err, ShouldBeNil)
			}

			gids := getPathGIDs(t, paths)
			So(gids, ShouldResemble, unchangedGIDs)
			So(buff.String(), ShouldBeBlank)

			So(testSetgidApplied(t, paths[2]), ShouldBeTrue)
			So(testSetgidApplied(t, paths[3]), ShouldBeFalse)

			So(is660(t, paths[0]), ShouldBeTrue)
			So(is660(t, paths[1]), ShouldBeFalse)
		})

		Convey("And a ch with a humgen rules", func() {
			humgenRule := dir + "\t*\t^\trw^rw^***\trw*rws***\n"
			rs, err := NewRulesStore().FromTSV(NewTSVReader(strings.NewReader(humgenRule)))
			So(err, ShouldBeNil)

			ch := New(rs, l)
			So(ch, ShouldNotBeNil)

			Convey("Do makes the desired changes if cb returns true", func() {
				for i, path := range paths {
					err := ch.Do(path, infos[i])
					So(err, ShouldBeNil)
				}

				gids := getPathGIDs(t, paths)
				So(gids, ShouldResemble, []int{otherGID, otherGID, otherGID, otherGID})
				So(buff.String(), ShouldContainSubstring, `lvl=info msg="changed ownership" path=`+paths[0])
				So(buff.String(), ShouldContainSubstring, fmt.Sprintf("origGroup=%s newGroup=%s", primaryName, otherName))

				So(testSetgidApplied(t, paths[2]), ShouldBeTrue)
				So(testSetgidApplied(t, paths[3]), ShouldBeTrue)
				So(buff.String(), ShouldContainSubstring, `lvl=info msg="set permissions" path=`+paths[3]+
					` old=drwxrwxrwx new=dgrwxrwxrwx`)

				So(is660(t, paths[0]), ShouldBeTrue)
				So(is660(t, paths[1]), ShouldBeTrue)
				So(buff.String(), ShouldContainSubstring, `lvl=info msg="set permissions" path=`+paths[1]+
					` old=-rw------- new=-rw-rw----`)
				So(buff.String(), ShouldNotContainSubstring, `lvl=info msg="set permissions" path=`+paths[0])
			})

			Convey("Do corrects -rw-rwxr-x to -rwxrwxr-x", func() {
				perm := createAndDoTestFile(t, dir, otherGID, 0675, ch)

				So(perm, ShouldEqual, "-rwxrwxr-x")
				So(buff.String(), ShouldContainSubstring, `lvl=info msg="set permissions" path=`)
			})

			Convey("Do corrects -rwxrw-r-x to -rwxrwxr-x", func() {
				perm := createAndDoTestFile(t, dir, otherGID, 0765, ch)

				So(perm, ShouldEqual, "-rwxrwxr-x")
				So(buff.String(), ShouldContainSubstring, `lvl=info msg="set permissions" path=`)
			})

			Convey("Do forces non-rw to ug+rw", func() {
				perm := createAndDoTestFile(t, dir, otherGID, 0440, ch)
				So(perm, ShouldEqual, "-rw-rw----")
				So(buff.String(), ShouldContainSubstring, `lvl=info msg="set permissions" path=`)

				perm = createAndDoTestFile(t, dir, otherGID, 0220, ch)
				So(perm, ShouldEqual, "-rw-rw----")

				perm = createAndDoTestFile(t, dir, otherGID, 0235, ch)
				So(perm, ShouldEqual, "-rwxrwxr-x")
			})

			Convey("Do on a non-existent path does nothing", func() {
				invalidPath := "/foo/bar"
				err := ch.Do(invalidPath, infos[2])
				So(err, ShouldBeNil)

				So(buff.String(), ShouldBeBlank)
			})

			Convey("Do on a bad path returns a set of errors", func() {
				badPath := createBadPath(t, dir)

				// cbGID = -2
				err := ch.Do(badPath, &badInfo{isDir: false})
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "1 error occurred")

				err = ch.Do(badPath, &badInfo{isDir: true})
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "2 errors occurred")

				err = ch.Do(badPath, &badInfo{isDir: false, perm: 9999})
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldContainSubstring, "2 errors occurred")
			})

			Convey("chownGroup applies to symlinks themselves, not their targets", func() {
				path := filepath.Join(dir, "e")
				slink := filepath.Join(dir, "f")

				createTestFile(t, path, primaryGID, 0660)
				err := os.Symlink(path, slink)
				So(err, ShouldBeNil)

				info, err := os.Lstat(slink)
				So(err, ShouldBeNil)

				err = ch.chown(rs.Get(path), slink, info)
				So(err, ShouldBeNil)

				info, err = os.Lstat(slink)
				So(err, ShouldBeNil)

				_, gid := getIDsFromFileInfo(info)
				So(gid, ShouldEqual, otherGID)

				Convey("chmod ignores symlinks but works on real files", func() {
					err = chmod(info, slink, 0670)
					So(err, ShouldBeNil)

					info, err = os.Lstat(path)
					So(info.Mode().Perm(), ShouldEqual, fs.FileMode(0660))

					err = chmod(info, path, 0670)
					So(err, ShouldBeNil)

					info, err = os.Lstat(path)
					So(info.Mode().Perm(), ShouldEqual, fs.FileMode(0670))
				})
			})
		})
	})
}

// getGIDs finds our primary GID and other GIDs of groups we belong to, so that
// we can test changing groups.
func getGIDs(t *testing.T) (int, []int) {
	t.Helper()

	primaryGID := os.Getgid()

	return primaryGID, getOtherGIDs(t, primaryGID)
}

// getOtherGIDs get's the current users's GroupIDs and returns those that
// aren't the same as the given GID.
func getOtherGIDs(t *testing.T, primaryGID int) []int {
	t.Helper()

	u, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}

	ugids, err := u.GroupIds()
	if err != nil {
		t.Fatal(err)
	}

	var gids []int

	for _, gid := range ugids {
		gid, err := strconv.ParseInt(gid, 10, 32)
		if err != nil {
			t.Fatal(err)
		}

		if int(gid) != primaryGID {
			gids = append(gids, int(gid))
		}
	}

	return gids
}

// testGroupName is a convienience function that calls groupName and Fatals on
// error.
func testGroupName(t *testing.T, gid int) string {
	t.Helper()

	name, err := groupName(gid)
	if err != nil {
		t.Fatal(err)
	}

	return name
}

// createTestFiles creates some files in a temp dir and returns their containing
// directory, paths and stats. The first belongs to primaryGID and has
// permissions 0660, the second belongs to otherGID and has permissions 0600,
// the 3rd is a directory that has the group sticky bit set, and the 4th is one
// that doesn't.
func createTestFiles(t *testing.T, primaryGID, otherGID int) (string, []string, []fs.FileInfo) {
	t.Helper()
	dir := t.TempDir()

	if err := os.Chown(dir, -1, otherGID); err != nil {
		t.Fatal(err)
	}

	p1 := filepath.Join(dir, "a")
	p2 := filepath.Join(dir, "b")
	p3 := filepath.Join(dir, "c")
	p4 := filepath.Join(dir, "d")

	i1 := createTestFile(t, p1, primaryGID, 0660)
	i2 := createTestFile(t, p2, otherGID, 0600)
	i3 := createTestDir(t, p3, true)
	i4 := createTestDir(t, p4, false)

	return dir, []string{p1, p2, p3, p4}, []fs.FileInfo{i1, i2, i3, i4}
}

// createTestFile creates the given empty file and sets its group to the given
// GID and applies the given perms. Returns stat of the file created. Fatal on
// error.
func createTestFile(t *testing.T, path string, gid int, perms fs.FileMode) fs.FileInfo {
	t.Helper()

	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}

	if err = f.Close(); err != nil {
		t.Fatal(err)
	}

	if err = os.Chown(path, -1, gid); err != nil {
		t.Fatal(err)
	}

	if err := os.Chmod(path, perms); err != nil {
		t.Fatal(err)
	}

	return statFile(t, path)
}

// statFile stats the given file. Fatal on error.
func statFile(t *testing.T, path string) fs.FileInfo {
	t.Helper()

	stat, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}

	return stat
}

// createTestDir creates the given directory and sets its group sticky bit if
// bool is true. Returns stat of the dir created. Fatal on error.
func createTestDir(t *testing.T, path string, sticky bool) fs.FileInfo {
	t.Helper()

	if err := os.Mkdir(path, os.ModePerm); err != nil {
		t.Fatal(err)
	}

	mode := os.ModePerm
	if sticky {
		mode |= os.ModeSetgid
	}

	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}

	return statFile(t, path)
}

// getPathGIDs gets the GIDs of the given paths.
func getPathGIDs(t *testing.T, paths []string) []int {
	t.Helper()

	gids := make([]int, len(paths))

	for i, path := range paths {
		gids[i] = getPathGID(t, path)
	}

	return gids
}

// getPathGID gets the GID of the given path.
func getPathGID(t *testing.T, path string) int {
	t.Helper()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	sys := info.Sys()
	stat, ok := sys.(*syscall.Stat_t)

	if !ok {
		t.Fatal("could not get syscall.Stat_t out of Stat attempt")
	}

	return int(stat.Gid)
}

// testSetgidApplied calls setgidApplied() by statting the given path first.
// Fatal on error.
func testSetgidApplied(t *testing.T, path string) bool {
	t.Helper()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	return (info.Mode() & os.ModeSetgid) != 0
}

// is660 tests if the file is user and group read/writable.
func is660(t *testing.T, path string) bool {
	t.Helper()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	return info.Mode().Perm() == 0660
}

// newLogger returns a logger that logs to the returned buffer.
func newLogger() (*bytes.Buffer, log15.Logger) { //nolint:ireturn
	buff := new(bytes.Buffer)
	l := log15.New()
	l.SetHandler(log15.StreamHandler(buff, log15.LogfmtFormat()))

	return buff, l
}

// createBadPath creates a directory with a path length greater than 4096, which
// should cause issues.
func createBadPath(t *testing.T, dir string) string {
	t.Helper()

	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		err = os.Chdir(wd)
		if err != nil {
			t.Fatal(err)
		}
	}()

	badPath := dir

	for i := 0; i < 17; i++ {
		err = os.Chdir(dir)
		if err != nil {
			t.Fatal(err)
		}

		err = os.Mkdir(longBasename, os.ModePerm)
		if err != nil {
			t.Fatal(err)
		}

		dir = longBasename
		badPath = filepath.Join(badPath, dir)
	}

	return badPath
}

// badInfo is an fs.FileInfo that has nonsense data.
type badInfo struct {
	isDir bool
	perm  int
}

func (b *badInfo) Name() string { return "foo" }

func (b *badInfo) Size() int64 { return -1 }

func (b *badInfo) Mode() fs.FileMode {
	if b.perm != 0 {
		return fs.FileMode(b.perm)
	}

	return os.ModePerm
}

func (b *badInfo) ModTime() time.Time { return time.Now() }

func (b *badInfo) IsDir() bool { return b.isDir }

func (b *badInfo) Sys() interface{} { return &syscall.Stat_t{Gid: 0} }

// createAndDoTestFile creates a temp file with given gid and perms,
// and calls ch.Do() on it. Set your callback to return true before calling
// this. Returns file permissions as a string afterwards.
func createAndDoTestFile(t *testing.T, dir string, otherGID int, perms fs.FileMode, ch *Ch) string {
	t.Helper()

	path := filepath.Join(dir, "a")
	info := createTestFile(t, path, otherGID, perms)

	err := ch.Do(path, info)
	So(err, ShouldBeNil)

	return getFilePermissions(t, path)
}

func getFilePermissions(t *testing.T, path string) string {
	t.Helper()

	info, err := os.Stat(path)
	So(err, ShouldBeNil)

	return info.Mode().Perm().String()
}
