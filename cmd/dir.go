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

package cmd

import (
	"io/fs"
	"os"

	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/hashdir"
	"github.com/wtsi-ssg/wrstat/stat"
)

// options for this cmd.
var outputDir string
var depGroup string

// dirCmd represents the dir command.
var dirCmd = &cobra.Command{
	Use:   "dir",
	Short: "Get stats on the contents of a directory",
	Long: `Get stats on the contents of a directory.

wr manager must have been started before running this.

Within the given output directory, hashed folders are created to contain the
output file.

For each file in the directory of interest, stats about the file are written to
the output file.

The output file format is 11 tab separated columns with the following contents:
1. Base64 encoded path to the file.
2. File size in bytes. If this is greater than the number of bytes in blocks
   allocated, this will be the number of bytes in allocated blocks. (This is to
   account for files with holes in them; as a byproduct, symbolic links will
   be reported as 0 size.)
3. UID.
4. GID.
5. Atime (time of most recent access expressed in seconds).
6. Mtime (time of most recent content modification expressed in seconds.)
7. Ctime (on unix, the time of most recent metadata change in seconds).
8. Filetype:
   'f': regular file
   'l': symbolic link
   's': socket
   'b': block special device file
   'c': character special device file
   'F': FIFO (named pipe)
   'X': anything else
9. Inode number (on unix).
10. Number of hard links.
11. Identifier of the device on which this file resides.

For each sub directory within the directory of interest, a job is added to wr's
queue that calls this command again with all the same arguments, changing only
the directory of interest to this sub directory. The job will be in the given
dependency group.

NB: when this exits, that does not mean all stats have necessarily been
retrieved. You should wait until all jobs in the given dependency group have
completed (eg. by adding your own job that depends on that group, such as a
'wrstat combine' call).`,
	Run: func(cmd *cobra.Command, args []string) {
		desiredDir := checkArgs(outputDir, depGroup, args)

		outFile := createOutputFile(outputDir, desiredDir)
		defer outFile.Close()

		files, _ := getFilesAndDirs(desiredDir)

		outputFileStats(outFile, desiredDir, files)

		die("not yet implemented")
	},
}

func init() {
	RootCmd.AddCommand(dirCmd)

	// flags specific to this sub-command
	dirCmd.Flags().StringVarP(&outputDir, "output_directory", "o", "", "base directory for output files")
	dirCmd.Flags().StringVarP(
		&depGroup,
		"dependency_group", "d", "",
		"dependency group that recursive jobs added to wr will belong to")
}

// checkArgs checks we have required args and returns desired dir.
func checkArgs(out, dep string, args []string) string {
	if out == "" {
		die("--output_directory is required")
	}

	if dep == "" {
		die("--dependecy_group is required")
	}

	if len(args) != 1 {
		die("exactly 1 directory of interest must be supplied")
	}

	return args[0]
}

// createOutputFile creates an output file within out in hashed location based
// on desired.
func createOutputFile(out, desired string) *os.File {
	h := hashdir.New(hashdir.RecommendedLevels)

	outFile, err := h.MkDirHashed(out, desired)
	if err != nil {
		die("failed to create output file: %s", err)
	}

	return outFile
}

// getFilesAndDirs reads the contents of the given directory and returns the
// file entries and the directory entries seperatly.
func getFilesAndDirs(desired string) ([]fs.DirEntry, []fs.DirEntry) {
	entries, err := os.ReadDir(desired)
	if err != nil {
		die("failed to read the contents of [%s]: %s", desired, err)
	}

	var files, dirs []fs.DirEntry

	for _, entry := range entries {
		if entry.IsDir() {
			dirs = append(dirs, entry)
		} else {
			files = append(files, entry)
		}
	}

	return files, dirs
}

// outputFileStats outputs file stats in our desired format to the output file.
func outputFileStats(out *os.File, desired string, files []fs.DirEntry) {
	for _, entry := range files {
		info, err := entry.Info()
		if err != nil {
			continue
		}

		_, err = out.WriteString(stat.File(desired, info).ToString())
		if err != nil {
			die("failed to write to output file: %s", err)
		}
	}
}
