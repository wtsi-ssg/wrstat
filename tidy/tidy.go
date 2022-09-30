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

type Error string

func (e Error) Error() string { return string(e) }

const modePermUser = 0700

// Can i put the const modeRW here?
const modeRW = 0666

// Up takes a source directory of wrstat output files and tidies them in to the
// given dest directory, using date in the filenames. If the dest dir doesn't
// exist, it will be created.
func Up(srcDir, destDir, date string) error {
	if err := dirValid(srcDir); err != nil {
		return err
	}

	err := dirValid(destDir)
	if os.IsNotExist(err) {
		err = os.Mkdir(destDir, modePermUser)

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

	return moveAndDelete(absSrcDir, absDestDir, destDirInfo, date)
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
func moveAndDelete(sourceDir, destDir string, destDirInfo fs.FileInfo, date string) error {
	combineSuffixes := map[string]string{
		"combine.stats.gz":       "stats.gz",
		"combine.byusergroup.gz": "byusergroup.gz",
		"combine.bygroup":        "bygroup",
		"combine.log.gz":         "logs.gz"}

	otherSuffixes := [2]string{"base.dirs", "combine.dgut.db"}

	for inSuffix, outSuffix := range combineSuffixes {
		if err := findAndMoveOutputs(sourceDir, destDir, destDirInfo, date, inSuffix, outSuffix); err != nil {
			return err
		}
	}

	if err := moveBaseDirsFile(sourceDir, destDir, destDirInfo, date, otherSuffixes[0]); err != nil {
		return err
	}

	if err := findAndMoveDBs(sourceDir, destDir, destDirInfo, date, otherSuffixes[1]); err != nil {
		return err
	}

	return os.RemoveAll(sourceDir)
}

// findAndMoveOutputs finds output files in the given sourceDir with given
// suffix and moves them to destDir, including date in the name, and adjusting
// ownership and permissions to match the destDir.
func findAndMoveOutputs(sourceDir, destDir string, destDirInfo fs.FileInfo,
	date, inputSuffix, outputSuffix string) error {
	outputPaths, err := filepath.Glob(fmt.Sprintf("%s/*/*/%s", sourceDir, inputSuffix))
	if err != nil {
		return err
	}

	err = moveOutputs(outputPaths, destDir, destDirInfo, date, outputSuffix)
	if err != nil {
		return err
	}

	return nil
}

// moveOutputs calls moveOutput() on each outputPaths source file.
func moveOutputs(outputPaths []string, destDir string, destDirInfo fs.FileInfo, date, suffix string) error {
	for _, path := range outputPaths {
		err := moveOutput(path, destDir, destDirInfo, date, suffix)
		if err != nil {
			return err
		}
	}

	return nil
}

// moveOutput moves an output file to the finalDir and changes its name to
// the correct format, then adjusts ownership and permissions to match the
// destDir.
func moveOutput(source string, destDir string, destDirInfo fs.FileInfo, date, suffix string) error {
	interestUniqueDir := filepath.Dir(source)
	interestBaseDir := filepath.Dir(interestUniqueDir)
	multiUniqueDir := filepath.Dir(interestBaseDir)
	dest := filepath.Join(destDir, fmt.Sprintf("%s_%s.%s.%s.%s",
		date,
		filepath.Base(interestBaseDir),
		filepath.Base(interestUniqueDir),
		filepath.Base(multiUniqueDir),
		suffix))

	return renameAndMatchPerms(source, dest, destDirInfo)
}

// renameAndMatchPerms tries 2 ways to rename the file (resorting to a copy if
// this is across filesystem boundaries), then matches the dest file permissions
// to the given FileInfo.
//
// If source doesn't exist, but dest does, assumes the rename was done
// previously and just tries to match the permissions.
func renameAndMatchPerms(source, dest string, destDirInfo fs.FileInfo) error {
	if _, err := os.Stat(source); errors.Is(err, os.ErrNotExist) {
		if _, err = os.Stat(dest); err == nil {
			return matchPerms(dest, destDirInfo)
		}
	}

	err := os.Rename(source, dest)
	if err != nil {
		if err = shutil.CopyFile(source, dest, false); err != nil {
			return err
		}
	}

	return matchPerms(dest, destDirInfo)
}

// matchPerms ensures that the given file has the same ownership and read-write
// permissions as the given fileinfo.
func matchPerms(path string, desired fs.FileInfo) error {
	current, err := os.Stat(path)
	if err != nil {
		return err
	}

	if err = matchOwnership(path, current, desired); err != nil {
		return err
	}

	return matchReadWrite(path, current, desired)
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
func matchReadWrite(path string, current, desired fs.FileInfo) error {
	currentMode := current.Mode()
	currentRW := currentMode & modeRW
	desiredRW := desired.Mode() & modeRW

	if currentRW == desiredRW {
		return nil
	}

	return os.Chmod(path, currentMode|desiredRW)
}

// moveBaseDirsFile moves the base.dirs file in sourceDir to a uniquely named
// .basedirs file in destDir that includes the given date.
func moveBaseDirsFile(sourceDir, destDir string, destDirInfo fs.FileInfo, date, basedirBasename string) error {
	source := filepath.Join(sourceDir, basedirBasename)

	dest := filepath.Join(destDir, fmt.Sprintf("%s_%s.basedirs",
		date,
		filepath.Base(sourceDir)))

	return renameAndMatchPerms(source, dest, destDirInfo)
}

// findAndMoveDBs finds the combine.dgut.db directories in the given sourceDir
// and moves them to a uniquely named dir in destDir that includes the given
// date, and adjusts ownership and permissions to match the destDir.
//
// It also touches a file that 'wrstat server' monitors to know when to reload
// its database files. It gives that file an mtime corresponding to the oldest
// mtime of the walk log files.
func findAndMoveDBs(sourceDir, destDir string, destDirInfo fs.FileInfo, date, combineDGUTOutputFileBasename string) error {
	sources, errg := filepath.Glob(fmt.Sprintf("%s/*/*/%s", sourceDir, combineDGUTOutputFileBasename))
	if errg != nil {
		return errg
	}

	dgutDBsSuffix := "dgut.dbs"

	dbsDir, err := makeDBsDir(sourceDir, destDir, destDirInfo, date, dgutDBsSuffix)
	if err != nil {
		return err
	}

	for i, source := range sources {
		if _, err = os.Stat(source); err != nil {
			return err
		}

		dest := filepath.Join(dbsDir, fmt.Sprintf("%d", i))

		err = renameAndMatchPerms(source, dest, destDirInfo)
		if err != nil {
			return err
		}
	}

	err = matchPermsInsideDir(dbsDir, destDirInfo)
	if err != nil {
		return err
	}

	dgutDBsSentinelBasename := ".dgut.dbs.updated"

	return touchDBUpdatedFile(sourceDir, destDir, destDirInfo, dgutDBsSentinelBasename)
}

// makeDBsDir makes a uniquely named directory featuring the given date to hold
// database files in destDir. If it already exists, does nothing. Returns the
// path to the database directory and any error.
func makeDBsDir(sourceDir, destDir string, destDirInfo fs.FileInfo, date, dgutDBsSuffix string) (string, error) {
	dbsDir := filepath.Join(destDir, fmt.Sprintf("%s_%s.%s",
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
func matchPermsInsideDir(dir string, desired fs.FileInfo) error {
	return filepath.WalkDir(dir, func(path string, de fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		return matchPerms(path, desired)
	})
}

// touchDBUpdatedFile touches a file that the server monitors so that it knows
// to try and reload the databases. Matches the permissions of the touched file
// to the given permissions. Gives the file an mtime corresponding to the oldest
// mtime of walk log files.
func touchDBUpdatedFile(sourceDir, destDir string, desired fs.FileInfo, dgutDBsSentinelBasename string) error {
	sentinel := filepath.Join(destDir, dgutDBsSentinelBasename)

	statLogOutputFileSuffix := ".log"

	oldest, err := getOldestMtimeOfWalkFiles(sourceDir, statLogOutputFileSuffix)
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

	return matchPerms(sentinel, desired)
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
func getOldestMtimeOfWalkFiles(dir, statLogOutputFileSuffix string) (time.Time, error) {
	paths, err := filepath.Glob(fmt.Sprintf("%s/*/*/*%s", dir, statLogOutputFileSuffix))
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
