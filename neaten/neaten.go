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
	fileCheck "github.com/wtsi-ssg/wrstat/v6/fs"
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
	SrcDir     string
	dotDestDir string
	DestDir    string

	// File suffixes of combine files in the SrcDir, and their counterpart in
	// the destDir.
	CombineFileSuffixes map[string]string

	// Glob pattern describing the path of combine files in SrcDir.
	CombineFileGlobPattern string

	// Glob pattern describing the path of walk files in SrcDir.
	WalkFilePathGlobPattern string

	// The perms of destdir if we make the destdir ourselves.
	DestDirPerms fs.FileMode

	destDirInfo fs.FileInfo
}

// Up takes our source directory of wrstat output files, renames them and
// relocates them to our dest directory, using our date. Also ensures that the
// permissions of wrstat output files match those of dest directory. If our dest
// dir doesn't exist, it will be created. And it touches a file called .updated,
// setting its mTime equal to the oldest of all those from our srcDir. Finally,
// deletes the source directory.
//
// For debugging purposes, set disableDeletion to true to disable deletion of
// the source directory after a successful move.
func (t *Tidy) Up(disableDeletion bool) error {
	if err := fileCheck.DirValid(t.SrcDir); err != nil {
		return err
	}

	t.dotDestDir = filepath.Join(filepath.Dir(t.DestDir), "."+filepath.Base(t.DestDir))

	if err := fileCheck.DirValid(t.DestDir); err != nil { //nolint:nestif
		if !os.IsNotExist(err) {
			return err
		}
	} else if err = os.Rename(t.DestDir, t.dotDestDir); err != nil {
		return err
	}

	err := fileCheck.DirValid(t.dotDestDir)
	if os.IsNotExist(err) {
		if err = os.MkdirAll(t.dotDestDir, t.DestDirPerms); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	t.destDirInfo, err = os.Stat(filepath.Dir(t.dotDestDir))
	if err != nil {
		return err
	}

	return t.moveAndDelete(disableDeletion)
}

// moveAndDelete does the main work of this package: move various files to our
// destDir, then delete our SrcDir if disableDeletion is false.
func (t *Tidy) moveAndDelete(disableDeletion bool) error {
	if err := t.move(); err != nil {
		return err
	}

	if disableDeletion {
		return t.matchPermsInsideDir(t.SrcDir)
	}

	if err := t.touchUpdatedFile(); err != nil {
		return err
	}

	return os.RemoveAll(t.SrcDir)
}

// move finds, renames and moves the combine, base and db files, ensuring that
// their permissions match those of our destDir.
func (t *Tidy) move() error {
	for inSuffix, outSuffix := range t.CombineFileSuffixes {
		if err := t.findAndMoveOutputs(inSuffix, outSuffix); err != nil {
			return err
		}
	}

	return nil
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
	return t.renameAndCorrectPerms(source, filepath.Join(t.dotDestDir, suffix))
}

// renameAndCorrectPerms tries 2 ways to rename the file (resorting to a copy if
// this is across filesystem boundaries), then matches the dest file permissions
// to those of our FileInfo.
//
// If source doesn't exist, but dest does, assumes the rename was done
// previously and just tries to match the permissions.
func (t *Tidy) renameAndCorrectPerms(source, dest string) error {
	s, err := os.Stat(source)
	if errors.Is(err, os.ErrNotExist) {
		if _, err = os.Stat(dest); err == nil {
			return CorrectPerms(dest, t.destDirInfo)
		}
	}

	err = os.Rename(source, dest)
	if err != nil {
		if s.IsDir() {
			return fs.ErrInvalid
		}

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

// matchPermsInsideDir does matchPerms for all the files in the given dir
// recursively.
func (t *Tidy) matchPermsInsideDir(dir string) error {
	return filepath.WalkDir(dir, func(path string, _ fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		return CorrectPerms(path, t.destDirInfo)
	})
}

// touchUpdatedFile touches the final output dir so that
// other processes can know when we're done. Matches the permissions of the
// touched file to the given permissions. Gives the file an mtime corresponding
// to the oldest mtime of walk log files.
func (t *Tidy) touchUpdatedFile() error {
	oldest, err := t.getOldestMtimeOfWalkFiles(t.SrcDir, ".log")
	if err != nil {
		return err
	}

	if err = changeAMFileTime(t.dotDestDir, oldest); err != nil {
		return err
	}

	return t.renameAndCorrectPerms(t.dotDestDir, t.DestDir)
}

// CreateFile creates a file in the given path.
func CreateFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	return file.Close()
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

// Touch modifies path's a and mtime to the current time.
func Touch(path string) error {
	now := time.Now().Local()

	return changeAMFileTime(path, now)
}

// DeleteAllPrefixedDirEntries deletes all files and directories in the given
// directory that have the given prefix.
func DeleteAllPrefixedDirEntries(dir, prefix string) error {
	paths, err := filepath.Glob(fmt.Sprintf("%s/%s*", dir, prefix))
	if err != nil {
		return err
	}

	for _, path := range paths {
		err = os.RemoveAll(path)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	return nil
}
