/*******************************************************************************
 * Copyright (c) 2021-2022 Genome Research Ltd.
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

package cmd

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	shutil "github.com/termie/go-shutil"
)

// modeRW are the read-write permission bits for user, group and other.
const modeRW = 0666

const dgutDBsBasename = "dgut.dbs"
const dgutDBsOldBasename = dgutDBsBasename + ".old"
const dgutDBsSentinelBasename = ".dgut.dbs.updated"

// options for this cmd.
var tidyDir string
var tidyDate string

// tidyCmd represents the tidy command.
var tidyCmd = &cobra.Command{
	Use:   "tidy",
	Short: "Tidy up multi output.",
	Long: `Tidy up multi output.

This is called by 'wrstat multi' after the main work has completed. It moves
final output files from the supplied unique working directory to the
--final_output directory, then deletes the working direcory.

multi creates a unique ("multi unique") directory , in which it creates
directories named after the basename of the directory of interest
("interest basename"), in which it creates another unique directory ("interest
unique"), in which it creates the output files.

tidy assumes the working directory you give it is the "multi unique" from multi.
It probably won't do the right thing if not.

Final output files are named to include the given --date as follows:
[date]_[interest basename].[interest unique].[multi unique].[suffix]

Where [suffix] is one of 'stats.gz', 'byusergroup.gz', 'bygroup' or 'logs.gz'.

The base.dirs file directly inside the given "multi unique" directory is named:
[date]_[multi unique].basedirs

It also moves the combine.dgut.db directories to inside a directory named:
dgut.dbs
(making them sequentially numbered sub-directories)
Note that in your --final_output directory, if a directory called dgut.dbs
exists, it will be moved aside to dgut.dbs.old and replaced with new data. If
you have a wrstat server using the database files inside, it will reload the
databases then delete the dgut.dbs.old directory.

The output files will be given the same user:group ownership and
user,group,other read & write permissions as the --final_output directory.

Once all output files have been moved, the "multi unique" directory is deleted.

It is safe to call this multiple times if it was, for example, killed half way
through; it won't clobber final outputs already moved. (With the exception of
failing during the final step of creating a new dgut.dbs folder, having moved
one aside; you'll have to delete dgut.dbs.old if it exists before retrying.)`,
	Run: func(cmd *cobra.Command, args []string) {
		if tidyDir == "" {
			die("--final_output is required")
		}
		if len(args) != 1 {
			die("exactly 1 unique working directory from 'wrstat multi' must be supplied")
		}

		destDir, err := filepath.Abs(tidyDir)
		if err != nil {
			die("could not determine absolute path to --final_output dir: %s", err)
		}

		err = os.MkdirAll(destDir, userOnlyPerm)
		if err != nil {
			die("failed to create --final_output dir [%s]: %s", destDir, err)
		}

		destDirInfo, err := os.Stat(destDir)
		if err != nil {
			die("could not stat the --final_output dir: %s", err)
		}

		sourceDir, err := filepath.Abs(args[0])
		if err != nil {
			die("could not determine absolute path to source dir: %s", err)
		}

		err = moveAndDelete(sourceDir, destDir, destDirInfo, tidyDate)
		if err != nil {
			die("failed to tidy: %s", err)
		}
	},
}

func init() {
	RootCmd.AddCommand(tidyCmd)

	// flags specific to this sub-command
	tidyCmd.Flags().StringVarP(&tidyDir, "final_output", "f", "", "final output directory")
	tidyCmd.Flags().StringVarP(&tidyDate, "date", "d", "", "datestamp of when 'wrstat multi' was called")
}

// moveAndDelete does the main work of this cmd.
func moveAndDelete(sourceDir, destDir string, destDirInfo fs.FileInfo, date string) error {
	if err := findAndMoveOutputs(sourceDir, destDir, destDirInfo, date,
		combineStatsOutputFileBasename, "stats.gz"); err != nil {
		return err
	}

	if err := findAndMoveOutputs(sourceDir, destDir, destDirInfo, date,
		combineUserGroupOutputFileBasename, "byusergroup.gz"); err != nil {
		return err
	}

	if err := findAndMoveOutputs(sourceDir, destDir, destDirInfo, date,
		combineGroupOutputFileBasename, "bygroup"); err != nil {
		return err
	}

	if err := findAndMoveOutputs(sourceDir, destDir, destDirInfo, date,
		combineLogOutputFileBasename, "logs.gz"); err != nil {
		return err
	}

	if err := moveBaseDirsFile(sourceDir, destDir, destDirInfo, date); err != nil {
		return err
	}

	if err := findAndMoveDBs(sourceDir, destDir, destDirInfo); err != nil {
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
func renameAndMatchPerms(source, dest string, destDirInfo fs.FileInfo) error {
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
func moveBaseDirsFile(sourceDir, destDir string, destDirInfo fs.FileInfo, date string) error {
	source := filepath.Join(sourceDir, basedirBasename)

	dest := filepath.Join(destDir, fmt.Sprintf("%s_%s.basedirs",
		date,
		filepath.Base(sourceDir)))

	return renameAndMatchPerms(source, dest, destDirInfo)
}

// findAndMoveDBs finds the combine.dgut.db directories in the given sourceDir
// and moves them to a fixed dir in destDir (first moving aside any dir with the
// same name in there), and adjusting ownership and permissions to match the
// destDir.
func findAndMoveDBs(sourceDir, destDir string, destDirInfo fs.FileInfo) error {
	sources, errg := filepath.Glob(fmt.Sprintf("%s/*/*/%s", sourceDir, combineDGUTOutputFileBasename))
	if errg != nil {
		return errg
	}

	dbsDir, err := makeDBsDir(destDir, destDirInfo)
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

	return touchDBUpdatedFile(destDir)
}

// makeDBsDir makes a directory in destDir to hold database files. It has a
// fixed name so that the server will have a single known location to load the
// databases from. If the directory already exists, first moves it aside.
func makeDBsDir(destDir string, destDirInfo fs.FileInfo) (string, error) {
	dbsDir := filepath.Join(destDir, dgutDBsBasename)

	err := os.Mkdir(dbsDir, destDirInfo.Mode().Perm())
	if os.IsExist(err) {
		err = os.Rename(dbsDir, filepath.Join(destDir, dgutDBsOldBasename))
		if err != nil {
			return "", err
		}

		err = os.Mkdir(dbsDir, destDirInfo.Mode().Perm())
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
// to try and reload the databases.
func touchDBUpdatedFile(destDir string) error {
	sentinel := filepath.Join(destDir, dgutDBsSentinelBasename)

	_, err := os.Stat(sentinel)
	if os.IsNotExist(err) {
		err = createFile(sentinel)
	} else {
		err = touchFile(sentinel)
	}

	return err
}

// createFile creates the give path.
func createFile(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}

	file.Close()

	return nil
}

// touchFile updates the a&mtime of the given path.
func touchFile(path string) error {
	now := time.Now().Local()

	return os.Chtimes(path, now, now)
}
