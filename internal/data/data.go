package internaldata

import (
	"io/fs"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wtsi-ssg/wrstat/v4/summary"
)

type stringBuilderCloser struct {
	strings.Builder
}

func (s stringBuilderCloser) Close() error {
	return nil
}

func TestDGUTData(t *testing.T, gidA, gidB, gidC, uidA, uidB int) string {
	t.Helper()

	dir := "/"
	abdf := filepath.Join(dir, "a", "b", "d", "f")
	abdg := filepath.Join(dir, "a", "b", "d", "g")
	abehtmp := filepath.Join(dir, "a", "b", "e", "h", "tmp")
	acd := filepath.Join(dir, "a", "c", "d")

	dgut := summary.NewByDirGroupUserType()
	doneDirs := make(map[string]bool)

	addTestFileInfo(t, dgut, doneDirs, filepath.Join(abdf, "file.cram"), 1, 10, gidA, uidA, 50, 50)
	addTestFileInfo(t, dgut, doneDirs, filepath.Join(abdg, "file.cram"), 2, 10, gidA, uidA, 60, 60)
	addTestFileInfo(t, dgut, doneDirs, filepath.Join(abdg, "file.cram"), 4, 10, gidA, uidB, 75, 75)
	addTestFileInfo(t, dgut, doneDirs, filepath.Join(dir, "a", "b", "e", "h", "file.bam"), 1, 5, gidA, uidA, 100, 30)
	addTestFileInfo(t, dgut, doneDirs, filepath.Join(abehtmp, "file.bam"), 1, 5, gidA, uidA, 80, 80)
	addTestFileInfo(t, dgut, doneDirs, filepath.Join(acd, "file.cram"), 5, 1, gidB, uidB, 90, 90)

	if gidC == 0 {
		addTestFileInfo(t, dgut, doneDirs, filepath.Join(dir, "a", "file.cram"), 1, 1, gidC, uidB, 50, 50)
		addTestFileInfo(t, dgut, doneDirs, filepath.Join(abdg, "file.cram"), 4, 10, gidA, uidB, 50, 75)
	}

	var sb stringBuilderCloser

	err := dgut.Output(&sb)
	if err != nil {
		t.Fatal(err)
	}

	return sb.String()
}

type fakeFileInfo struct {
	dir  bool
	stat *syscall.Stat_t
}

func (f *fakeFileInfo) Name() string       { return "" }
func (f *fakeFileInfo) Size() int64        { return f.stat.Size }
func (f *fakeFileInfo) Mode() fs.FileMode  { return 0 }
func (f *fakeFileInfo) ModTime() time.Time { return time.Time{} }
func (f *fakeFileInfo) IsDir() bool        { return f.dir }
func (f *fakeFileInfo) Sys() any           { return f.stat }

func addTestFileInfo(t *testing.T, dgut *summary.DirGroupUserType, doneDirs map[string]bool,
	path string, numFiles, sizeOfEachFile, gid, uid, atime, mtime int) {
	t.Helper()

	dir, basename := filepath.Split(path)

	for i := 0; i < numFiles; i++ {
		filePath := filepath.Join(dir, strconv.FormatInt(int64(i), 10)+basename)

		info := &fakeFileInfo{
			stat: &syscall.Stat_t{
				Uid:  uint32(uid),
				Gid:  uint32(gid),
				Size: int64(sizeOfEachFile),
				Atim: syscall.Timespec{Sec: int64(atime)},
				Mtim: syscall.Timespec{Sec: int64(mtime)},
			},
		}

		err := dgut.Add(filePath, info)
		if err != nil {
			t.Fatal(err)
		}
	}

	addTestDirInfo(t, dgut, doneDirs, filepath.Dir(path), gid, uid)
}

func addTestDirInfo(t *testing.T, dgut *summary.DirGroupUserType, doneDirs map[string]bool,
	dir string, gid, uid int) {
	t.Helper()

	for {
		if doneDirs[dir] {
			return
		}

		info := &fakeFileInfo{
			dir: true,
			stat: &syscall.Stat_t{
				Uid:  uint32(uid),
				Gid:  uint32(gid),
				Size: int64(1024),
				Mtim: syscall.Timespec{Sec: int64(1)},
			},
		}

		err := dgut.Add(dir, info)
		if err != nil {
			t.Fatal(err)
		}

		doneDirs[dir] = true

		dir = filepath.Dir(dir)
		if dir == "/" {
			return
		}
	}
}
