/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
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
	"path/filepath"

	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/v5/neaten"
)

const uniqueLen = 20

// options for this cmd.
var cleanupDir string
var cleanupPerms bool

// cleanupCmd represents the cleanup command.
var cleanupCmd = &cobra.Command{
	Use:   "cleanup",
	Short: "Cleanup up aborted multi working files.",
	Long: `Cleanup up aborted multi working files.

If you use 'wrstat multi' but the run fails, you will be left with data in the
--working_directory. If you ran with --sudo, you might not have permission to
delete the data yourself.

By providing the same --working_directory to this command and using sudo, you
can delete the data easily.

Alternatively, to debug an issue you can provide the --perms flag to make all
the sub directories and their files match the perms of the working directory,
instead of deleting them.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if cleanupDir == "" {
			die("--working_directory is required")
		}

		if cleanupPerms {
			err := matchPerms(cleanupDir)
			if err != nil {
				die("could not correct permissions: %s", err)
			}
		} else {
			err := cleanup(cleanupDir)
			if err != nil {
				die("could not cleanup dir: %s", err)
			}
		}
	},
}

func init() {
	RootCmd.AddCommand(cleanupCmd)

	// flags specific to this sub-command
	cleanupCmd.Flags().StringVarP(&cleanupDir, "working_directory", "w", "",
		"base directory supplied to multi for intermediate results")
	cleanupCmd.Flags().BoolVarP(&cleanupPerms, "perms", "p", false,
		"instead of deleting them, make working subdirectory permissions match the working directory")
}

func matchPerms(workDir string) error {
	subDirs, err := getWorkingSubDirs(workDir)
	if err != nil {
		return err
	}

	desired, err := os.Stat(workDir)
	if err != nil {
		return err
	}

	for _, path := range subDirs {
		err = filepath.WalkDir(path, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}

			return neaten.CorrectPerms(path, desired)
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func getWorkingSubDirs(workDir string) ([]string, error) {
	entries, err := os.ReadDir(workDir)
	if err != nil {
		return nil, err
	}

	var paths []string //nolint:prealloc

	for _, e := range entries {
		if !e.IsDir() || len(e.Name()) != uniqueLen {
			continue
		}

		path := filepath.Join(workDir, e.Name())
		paths = append(paths, path)
	}

	return paths, nil
}

func cleanup(workDir string) error {
	subDirs, err := getWorkingSubDirs(workDir)
	if err != nil {
		return err
	}

	for _, path := range subDirs {
		if err = os.RemoveAll(path); err != nil {
			return err
		}
	}

	return nil
}
