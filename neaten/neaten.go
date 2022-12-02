/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * Author: Kyle Mace <km34@sanger.ac.uk>
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

package neaten

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/termie/go-shutil"
	fileCheck "github.com/wtsi-ssg/wrstat/v3/fs"
)

type Error string

func (e Error) Error() string { return string(e) }

const ErrNoOutputsFound = Error("There are no existing files according to the provided input and output suffixes.")

// modeRW are the read-write permission bits for user, group and other.
const modeRW = 0666

// Up struct defines your source directory, suffixes and glob patterns to find
// input files, and information about your destination directory, so that Up()
// can tidy your source files to the DestDir.
type Tidy struct {
	SrcDir  string
	DestDir string

	// Date used in the renaming of files.
	Date string

	// File suffixes of combine files in the SrcDir, and their counterpart in
	// the destDir.
	CombineFileSuffixes map[string]string

	// File suffixes of db files in the SrcDir, and their counterpart in the
	// destDir.
	DBFileSuffixes map[string]string

	// File suffixes of base files in the SrcDir, and their counterpart in the
	// destDir.
	BaseFileSuffixes map[string]string

	// Glob pattern describing the path of combine files in SrcDir.
	CombineFileGlobPattern string

	// Glob pattern describing the path of db files in SrcDir.
	DBFileGlobPattern string

	// Glob pattern describing the path of walk files in SrcDir.
	WalkFilePathGlobPattern string

	// The perms of destdir if we make the destdir ourselves.
	DestDirPerms fs.FileMode

	destDirInfo fs.FileInfo
}

// Up takes our source directory of wrstat output files, renames them and
// relocates them to our dest directory, using our date. Also ensures that the
// permissions of wrstat output files match those of dest directory. If our dest
// dir doesn't exist, it will be created. And it touches a file called
// .dgut.db.updated, setting its mTime equal to the oldest of all those from our
// srcDir.
func (t *Tidy) Up() error {
	if err := fileCheck.DirValid(t.SrcDir); err != nil {
		return err
	}

	err := fileCheck.DirValid(t.DestDir)
	if os.IsNotExist(err) {
		err = os.MkdirAll(t.DestDir, t.DestDirPerms)
		if err != nil {
			return err
		}
	}

	t.destDirInfo, err = os.Stat(t.DestDir)
	if err != nil {
		return err
	}

	return t.moveAndDelete()
}

// moveAndDelete does the main work of this package: it finds, renames and moves
// the combine, base and db files, ensuring that their permissions match those
// of our destDir.
func (t *Tidy) moveAndDelete() error {
	for inSuffix, outSuffix := range t.CombineFileSuffixes {
		if err := t.findAndMoveOutputs(inSuffix, outSuffix); err != nil {
			return err
		}
	}

	for inSuffix, outSuffix := range t.DBFileSuffixes {
		if err := t.findAndMoveDBs(inSuffix, outSuffix); err != nil {
			return err
		}
	}

	for inSuffix, outSuffix := range t.BaseFileSuffixes {
		if err := t.moveBaseDirsFile(inSuffix, outSuffix); err != nil {
			return err
		}
	}

	return os.RemoveAll(t.SrcDir)
}

// findAndMoveOutputs finds output files in the given sourceDir with given
// suffix and moves them to our destDir, including date in the name, and adjusts
// ownership and permissions to match the destDir.
func (t *Tidy) findAndMoveOutputs(inSuffix, outSuffix string) error {
	outputPaths, err := filepath.Glob(fmt.Sprintf(t.CombineFileGlobPattern, t.SrcDir, inSuffix))
	if err != nil {
		return err
	}

	if len(outputPaths) == 0 {
		return ErrNoOutputsFound
	}

	for _, path := range outputPaths {
		err := t.moveOutput(path, outSuffix)
		if err != nil {
			return err
		}
	}

	return nil
}

// moveOutput moves an output file to our desrDir and changes its name to the
// correct format, then adjusts ownership and permissions to match the destDir.
func (t *Tidy) moveOutput(source string, suffix string) error {
	interestUniqueDir := filepath.Dir(source)
	interestBaseDir := filepath.Dir(interestUniqueDir)
	multiUniqueDir := filepath.Dir(interestBaseDir)
	dest := filepath.Join(t.DestDir, fmt.Sprintf("%s_%s.%s.%s.%s",
		t.Date,
		filepath.Base(interestBaseDir),
		filepath.Base(interestUniqueDir),
		filepath.Base(multiUniqueDir),
		suffix))

	return t.renameAndCorrectPerms(source, dest)
}

// renameAndCorrectPerms tries 2 ways to rename the file (resorting to a copy if
// this is across filesystem boundaries), then matches the dest file permissions
// to those of our FileInfo.
//
// If source doesn't exist, but dest does, assumes the rename was done
// previously and just tries to match the permissions.
func (t *Tidy) renameAndCorrectPerms(source, dest string) error {
	if _, err := os.Stat(source); errors.Is(err, os.ErrNotExist) {
		if _, err = os.Stat(dest); err == nil {
			return CorrectPerms(dest, t.destDirInfo)
		}
	}

	err := os.Rename(source, dest)
	if err != nil {
		if err = shutil.CopyFile(source, dest, false); err != nil {
			return err
		}
	}

	return CorrectPerms(dest, t.destDirInfo)
}

// CorrectPerms checks whether the given file has the same ownership and
// read-write permissions as the given destDir info. If permissions do not
// match, they will be changed accordingly.
func CorrectPerms(path string, destDirInfo fs.FileInfo) error {
	current, err := os.Stat(path)
	if err != nil {
		return err
	}

	if err = matchOwnership(path, current, destDirInfo); err != nil {
		return err
	}

	return matchReadWrite(path, current, destDirInfo)
}

// ownershipMatches checks whether the given file with the current fileinfo has
// the same user and group ownership as the desired fileinfo. If the user and
// group ownerships do not match, they will be changed accordingly.
func matchOwnership(path string, current, desired fs.FileInfo) error {
	uid, gid := getUIDAndGID(current)
	desiredUID, desiredGID := getUIDAndGID(desired)

	if uid == desiredUID && gid == desiredGID {
		return nil
	}

	return os.Lchown(path, desiredUID, desiredGID)
}

// getUIDAndGID extracts the UID and GID from a FileInfo. NB: this will only
// work on linux.
func getUIDAndGID(info fs.FileInfo) (int, int) {
	return int(info.Sys().(*syscall.Stat_t).Uid), int(info.Sys().(*syscall.Stat_t).Gid) //nolint:forcetypeassert
}

// matchReadWrite checks whether the given file with the current fileinfo has
// the same user, group, other read&write permissions as our destDir. If they do
// not match they will be changed accordingly.
func matchReadWrite(path string, current, destDirInfo fs.FileInfo) error {
	currentMode := current.Mode()
	currentRW := currentMode & modeRW
	desiredRW := destDirInfo.Mode() & modeRW

	if currentRW == desiredRW {
		return nil
	}

	return os.Chmod(path, currentMode|desiredRW)
}

// moveBaseDirsFile moves the base.dirs file in sourceDir to a uniquely named
// .basedirs file in destDir that includes our date.
func (t *Tidy) moveBaseDirsFile(inSuffix, outSuffix string) error {
	source := filepath.Join(t.SrcDir, inSuffix)

	dest := filepath.Join(t.DestDir, fmt.Sprintf("%s_%s.%s",
		t.Date,
		filepath.Base(t.SrcDir),
		outSuffix))

	return t.renameAndCorrectPerms(source, dest)
}

// findAndMoveDBs finds the combine.dgut.db directories in our sourceDir and
// moves them to a uniquely named dir in destDir that includes our date, and
// adjusts ownership and permissions to match our destDir.
//
// It also touches a file that 'wrstat server' monitors to know when to reload
// its database files. It gives that file an mtime corresponding to the oldest
// mtime of the walk log files.
func (t *Tidy) findAndMoveDBs(inSuffix, outSuffix string) error {
	sources, err := filepath.Glob(fmt.Sprintf(t.DBFileGlobPattern, t.SrcDir, inSuffix))
	if err != nil {
		return err
	}

	dbsDir, err := t.makeDBsDir(outSuffix)
	if err != nil {
		return err
	}

	for i, source := range sources {
		if _, err = os.Stat(source); err != nil {
			return err
		}

		dest := filepath.Join(dbsDir, fmt.Sprintf("%d", i))

		err = t.renameAndCorrectPerms(source, dest)
		if err != nil {
			return err
		}
	}

	err = t.matchPermsInsideDir(dbsDir)
	if err != nil {
		return err
	}

	return t.touchDBUpdatedFile("." + outSuffix + ".updated")
}

// makeDBsDir makes a uniquely named directory featuring the given date to hold
// database files in destDir. If it already exists, does nothing. Returns the
// path to the database directory and any error.
func (t *Tidy) makeDBsDir(dgutDBsSuffix string) (string, error) {
	dbsDir := filepath.Join(t.DestDir, fmt.Sprintf("%s_%s.%s",
		t.Date,
		filepath.Base(t.SrcDir),
		dgutDBsSuffix,
	))

	err := os.Mkdir(dbsDir, t.destDirInfo.Mode().Perm())
	if os.IsExist(err) {
		err = nil
	}

	return dbsDir, err
}

// matchPermsInsideDir does matchPerms for all the files in the given dir
// recursively.
func (t *Tidy) matchPermsInsideDir(dir string) error {
	return filepath.WalkDir(dir, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		return CorrectPerms(path, t.destDirInfo)
	})
}

// touchDBUpdatedFile touches a file that the server monitors so that it knows
// to try and reload the databases. Matches the permissions of the touched file
// to the given permissions. Gives the file an mtime corresponding to the oldest
// mtime of walk log files.
func (t *Tidy) touchDBUpdatedFile(dgutDBsSentinelBasename string) error {
	sentinel := filepath.Join(t.DestDir, dgutDBsSentinelBasename)

	oldest, err := t.getOldestMtimeOfWalkFiles(t.SrcDir, ".log")
	if err != nil {
		return err
	}

	_, err = os.Stat(sentinel)
	if os.IsNotExist(err) {
		if err = createFile(sentinel); err != nil {
			return err
		}
	}

	if err = changeAMFileTime(sentinel, oldest); err != nil {
		return err
	}

	return CorrectPerms(sentinel, t.destDirInfo)
}

// createFile creates a file in the given path.
func createFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	file.Close()

	return nil
}

// changeAMFileTime updates the a&m time of the given path to the given time.
func changeAMFileTime(path string, t time.Time) error {
	return os.Chtimes(path, t.Local(), t.Local())
}

// getOldestMtimeOfWalkFiles looks in our sourceDir for walk log files and
// returns the oldest mtime of them all.
func (t *Tidy) getOldestMtimeOfWalkFiles(dir, statLogOutputFileSuffix string) (time.Time, error) {
	paths, err := filepath.Glob(fmt.Sprintf(t.WalkFilePathGlobPattern, dir, statLogOutputFileSuffix))
	if err != nil || len(paths) == 0 {
		return time.Now(), err
	}

	oldestT := time.Now()

	for _, path := range paths {
		info, err := os.Stat(path)
		if err != nil {
			return time.Time{}, err
		}

		if info.ModTime().Before(oldestT) {
			oldestT = info.ModTime()
		}
	}

	return oldestT, nil
}
