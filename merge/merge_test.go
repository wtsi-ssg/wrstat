package merge

import (
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/rs/xid"
	. "github.com/smartystreets/goconvey/convey"
	internaldb "github.com/wtsi-ssg/wrstat/v4/internal/db"
	ifs "github.com/wtsi-ssg/wrstat/v4/internal/fs"
	"github.com/wtsi-ssg/wrstat/v4/neaten"
)

func TestMerge(t *testing.T) {
	Convey("", t, func() {
		times := [...]time.Time{
			time.Now().Add(-3 * time.Hour),
			time.Now().Add(-2 * time.Hour),
			time.Now().Add(-1 * time.Hour),
		}
		multiOutput := t.TempDir()
		customOutput := t.TempDir()
		multiUnique := filepath.Join(multiOutput, xid.NewWithTime(times[0]).String())
		uniqueOutput := filepath.Join(multiUnique, "path1")
		oldCustom := filepath.Join(customOutput, xid.NewWithTime(times[1]).String())
		newCustom := filepath.Join(customOutput, xid.NewWithTime(times[2]).String())
		extraFiles := [...]string{"orig", "bad", "good"}

		for i, path := range [...]string{
			uniqueOutput,
			oldCustom,
			newCustom,
		} {
			err := os.MkdirAll(path, internaldb.DirPerms)
			So(err, ShouldBeNil)

			err = createDirStructure(path, times[i], extraFiles[i])
			So(err, ShouldBeNil)
		}

		err := Merge(customOutput, multiUnique, true)
		So(err, ShouldBeNil)

		err = checkFile(oldCustom)
		So(err, ShouldNotBeNil)

		err = checkFile(newCustom)
		So(err, ShouldBeNil)

		err = checkFile(filepath.Join(multiUnique, "bad"))
		So(err, ShouldNotBeNil)

		err = checkFile(filepath.Join(multiUnique, "good"))
		So(err, ShouldBeNil)
	})
}

func TestCopy(t *testing.T) {
	Convey("Given a directory tree", t, func() {
		dir := t.TempDir()
		newDir := t.TempDir()

		err := createDirStructure(dir, time.Now().Add(time.Second*-10))
		So(err, ShouldBeNil)

		err = copyPreservingTimestamp(dir, newDir)
		So(err, ShouldBeNil)

		err = fs.WalkDir(os.DirFS(dir), ".", func(path string, de fs.DirEntry, errr error) error {
			if errr != nil {
				return errr
			}

			fs, errrr := de.Info()
			So(errrr, ShouldBeNil)

			mt := fs.ModTime()

			fs, errrr = os.Lstat(filepath.Join(newDir, path))
			So(errrr, ShouldBeNil)

			So(fs.ModTime(), ShouldEqual, mt)

			So(de.Type(), ShouldEqual, fs.Mode().Type())

			return nil
		})
		So(err, ShouldBeNil)
	})
}

func createDirStructure(base string, mt time.Time, extra ...string) error {
	dirs := [...]string{".", "a", "b", "b/c", "b/d"}

	for _, dir := range dirs[1:] {
		p := filepath.Join(base, dir)

		if err := os.Mkdir(p, 0755); err != nil {
			return err
		}
	}

	for _, file := range append(extra, "aFile", "a/notherFile", "b/d/e", "b/"+watchFile) {
		p := filepath.Join(base, file)

		err := neaten.CreateFile(p)
		if err != nil {
			return err
		}

		if err := ifs.Touch(p, mt); err != nil {
			return err
		}
	}

	slices.Reverse(dirs[:])

	for _, dir := range dirs {
		if err := ifs.Touch(filepath.Join(base, dir), mt); err != nil {
			return err
		}
	}

	return nil
}

func checkFile(path string) error {
	_, err := os.Lstat(path)

	return err
}
