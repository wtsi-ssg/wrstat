/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 *         Kyle Mace  <km34@sanger.ac.uk>
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

package tidy

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/termie/go-shutil"
)

// Should I put the constants in the up structure too?

type Up struct {
	CombineFileSuffixes map[string]string
	DBFileSuffixes      map[string]string
	BaseFileSuffixes    map[string]string
	CombineFilePaths    [2]string
	DBFilePaths         [2]string
	BaseFilePath        string
	WalkFilePath        string
	modePermUser        fs.FileMode
	modeRW              fs.FileMode
}

type Error string

func (e Error) Error() string { return string(e) }

// Up takes a source directory of wrstat output files and tidies them in to the
// given dest directory, using date in the filenames. If the dest dir doesn't
// exist, it will be created.
func (u *Up) Up(srcDir, destDir, date string) error {
	if err := dirValid(srcDir); err != nil {
		return err
	}

	err := dirValid(destDir)
	if os.IsNotExist(err) {
		err = os.Mkdir(destDir, u.modePermUser)

		return err
	}

	destDirInfo, err := os.Stat(destDir)
	if err != nil {
		return err
	}

	absSrcDir, err := filepath.Abs(srcDir)
	if err != nil {
		return err
	}

	absDestDir, err := filepath.Abs(destDir)
	if err != nil {
		return err
	}

	return u.moveAndDelete(absSrcDir, absDestDir, destDirInfo, date)
}

// Checks if the directory is valid; exists or not.
func dirValid(addr string) error {
	addr, err := filepath.Abs(addr)
	if err != nil {
		return err
	}

	_, err = os.Stat(addr)

	return err
}

// moveAndDelete does the main work of this cmd.
func (u *Up) moveAndDelete(sourceDir, destDir string, destDirInfo fs.FileInfo, date string) error {
	for inSuffix, outSuffix := range u.CombineFileSuffixes {
		if err := u.findAndMoveOutputs(sourceDir, destDir, destDirInfo, inSuffix, outSuffix, date); err != nil {
			return err
		}
	}

	for inSuffix, outSuffix := range u.DBFileSuffixes {
		if err := u.findAndMoveDBs(sourceDir, destDir, destDirInfo, inSuffix, outSuffix, date); err != nil {
			return err
		}
	}

	for inSuffix, outSuffix := range u.BaseFileSuffixes {
		if err := u.moveBaseDirsFile(sourceDir, destDir, destDirInfo, inSuffix, outSuffix, date); err != nil {
			return err
		}
	}

	return os.RemoveAll(sourceDir)
}

// findAndMoveOutputs finds output files in the given sourceDir with given
// suffix and moves them to destDir, including date in the name, and adjusting
// ownership and permissions to match the destDir.
func (u *Up) findAndMoveOutputs(sourceDir, destDir string, destDirInfo fs.FileInfo, inSuffix, outSuffix, date string) error {
	outputPaths, err := filepath.Glob(fmt.Sprintf(u.CombineFilePaths[0], sourceDir, inSuffix))
	if err != nil {
		return err
	}

	for _, path := range outputPaths {
		err := u.moveOutput(path, destDir, destDirInfo, date, outSuffix)
		if err != nil {
			return err
		}
	}

	return nil
}

// moveOutput moves an output file to the finalDir and changes its name to
// the correct format, then adjusts ownership and permissions to match the
// destDir.
func (u *Up) moveOutput(source string, destDir string, destDirInfo fs.FileInfo, date, suffix string) error {
	interestUniqueDir := filepath.Dir(source)
	interestBaseDir := filepath.Dir(interestUniqueDir)
	multiUniqueDir := filepath.Dir(interestBaseDir)
	dest := filepath.Join(destDir, fmt.Sprintf(u.CombineFilePaths[1],
		date,
		filepath.Base(interestBaseDir),
		filepath.Base(interestUniqueDir),
		filepath.Base(multiUniqueDir),
		suffix))

	return u.renameAndMatchPerms(source, dest, destDirInfo)
}

// renameAndMatchPerms tries 2 ways to rename the file (resorting to a copy if
// this is across filesystem boundaries), then matches the dest file permissions
// to the given FileInfo.
//
// If source doesn't exist, but dest does, assumes the rename was done
// previously and just tries to match the permissions.
func (u *Up) renameAndMatchPerms(source, dest string, destDirInfo fs.FileInfo) error {
	if _, err := os.Stat(source); errors.Is(err, os.ErrNotExist) {
		if _, err = os.Stat(dest); err == nil {
			return u.matchPerms(dest, destDirInfo)
		}
	}

	err := os.Rename(source, dest)
	if err != nil {
		if err = shutil.CopyFile(source, dest, false); err != nil {
			return err
		}
	}

	return u.matchPerms(dest, destDirInfo)
}

// matchPerms ensures that the given file has the same ownership and read-write
// permissions as the given fileinfo.
func (u *Up) matchPerms(path string, desired fs.FileInfo) error {
	current, err := os.Stat(path)
	if err != nil {
		return err
	}

	if err = matchOwnership(path, current, desired); err != nil {
		return err
	}

	return u.matchReadWrite(path, current, desired)
}

// matchOwnership ensures that the given file with the current fileinfo has the
// same user and group ownership as the desired fileinfo.
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

// matchReadWrite ensures that the given file with the current fileinfo has the
// same user,group,other read&write permissions as the desired fileinfo.
func (u *Up) matchReadWrite(path string, current, desired fs.FileInfo) error {
	currentMode := current.Mode()
	currentRW := currentMode & u.modeRW
	desiredRW := desired.Mode() & u.modeRW

	if currentRW == desiredRW {
		return nil
	}

	return os.Chmod(path, currentMode|desiredRW)
}

// moveBaseDirsFile moves the base.dirs file in sourceDir to a uniquely named
// .basedirs file in destDir that includes the given date.
func (u *Up) moveBaseDirsFile(sourceDir, destDir string, destDirInfo fs.FileInfo, inSuffix, outSuffix, date string) error {
	source := filepath.Join(sourceDir, inSuffix)

	dest := filepath.Join(destDir, fmt.Sprintf(u.BaseFilePath,
		date,
		filepath.Base(sourceDir),
		outSuffix))

	return u.renameAndMatchPerms(source, dest, destDirInfo)
}

// findAndMoveDBs finds the combine.dgut.db directories in the given sourceDir
// and moves them to a uniquely named dir in destDir that includes the given
// date, and adjusts ownership and permissions to match the destDir.
//
// It also touches a file that 'wrstat server' monitors to know when to reload
// its database files. It gives that file an mtime corresponding to the oldest
// mtime of the walk log files.
func (u *Up) findAndMoveDBs(sourceDir, destDir string, destDirInfo fs.FileInfo, inSuffix, outSuffix, date string) error {
	sources, errg := filepath.Glob(fmt.Sprintf(u.DBFilePaths[0], sourceDir, inSuffix))
	if errg != nil {
		return errg
	}

	dbsDir, err := u.makeDBsDir(sourceDir, destDir, destDirInfo, date, outSuffix)
	if err != nil {
		return err
	}

	for i, source := range sources {
		if _, err = os.Stat(source); err != nil {
			return err
		}

		dest := filepath.Join(dbsDir, fmt.Sprintf("%d", i))

		err = u.renameAndMatchPerms(source, dest, destDirInfo)
		if err != nil {
			return err
		}
	}

	err = u.matchPermsInsideDir(dbsDir, destDirInfo)
	if err != nil {
		return err
	}

	return u.touchDBUpdatedFile(sourceDir, destDir, destDirInfo, "."+outSuffix+".updated")
}

// makeDBsDir makes a uniquely named directory featuring the given date to hold
// database files in destDir. If it already exists, does nothing. Returns the
// path to the database directory and any error.
func (u *Up) makeDBsDir(sourceDir, destDir string, destDirInfo fs.FileInfo, date, dgutDBsSuffix string) (string, error) {
	dbsDir := filepath.Join(destDir, fmt.Sprintf(u.DBFilePaths[1],
		date,
		filepath.Base(sourceDir),
		dgutDBsSuffix,
	))

	err := os.Mkdir(dbsDir, destDirInfo.Mode().Perm())
	if os.IsExist(err) {
		err = nil
	}

	return dbsDir, err
}

// matchPermsInsideDir does matchPerms for all the files in the given dir
// recursively.
func (u *Up) matchPermsInsideDir(dir string, desired fs.FileInfo) error {
	return filepath.WalkDir(dir, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		return u.matchPerms(path, desired)
	})
}

// touchDBUpdatedFile touches a file that the server monitors so that it knows
// to try and reload the databases. Matches the permissions of the touched file
// to the given permissions. Gives the file an mtime corresponding to the oldest
// mtime of walk log files.
func (u *Up) touchDBUpdatedFile(sourceDir, destDir string, desired fs.FileInfo, dgutDBsSentinelBasename string) error {
	sentinel := filepath.Join(destDir, dgutDBsSentinelBasename)

	oldest, err := u.getOldestMtimeOfWalkFiles(sourceDir, ".log")
	if err != nil {
		return err
	}

	_, err = os.Stat(sentinel)
	if os.IsNotExist(err) {
		if err = createFile(sentinel); err != nil {
			return err
		}
	}

	if err = touchFile(sentinel, oldest); err != nil {
		return err
	}

	return u.matchPerms(sentinel, desired)
}

// createFile creates the given path.
func createFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	file.Close()

	return nil
}

// touchFile updates the a&mtime of the given path to the given time.
func touchFile(path string, t time.Time) error {
	return os.Chtimes(path, t.Local(), t.Local())
}

// getOldestMtimeOfWalkFiles looks in sourceDir for walk log files and returns
// their oldest mtime.
func (u *Up) getOldestMtimeOfWalkFiles(dir, statLogOutputFileSuffix string) (time.Time, error) {
	paths, err := filepath.Glob(fmt.Sprintf(u.WalkFilePath, dir, statLogOutputFileSuffix))
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
