/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * Author: Ash Holland <ah37@sanger.ac.uk>
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
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/otiai10/copy"
)

// MergeDGUTDBDirectories copies the dgut.db numbered subdirectories from inside
// sourceDir to inside destDir, renaming them so they don't conflict with
// existing destDir subdirectories.
func MergeDGUTDBDirectories(sourceDir, destDir string) error {
	highestSubdirNum, err := getHighestNumberSubdir(destDir)
	if err != nil {
		return err
	}

	return copySourceDirsToDestDir(sourceDir, destDir, highestSubdirNum+1)
}

func getHighestNumberSubdir(destDir string) (int, error) {
	destEntries, err := os.ReadDir(destDir)
	if err != nil {
		return 0, err
	}

	var highestSubdirNum int

	for _, entry := range destEntries {
		num, erra := strconv.Atoi(entry.Name())
		if erra != nil {
			return 0, erra
		}

		if num > highestSubdirNum {
			highestSubdirNum = num
		}
	}

	return highestSubdirNum, nil
}

func copySourceDirsToDestDir(sourceDir, destDir string, nextDestSubDirNum int) error {
	sourceEntries, err := os.ReadDir(sourceDir)
	if err != nil {
		return err
	}

	for i, entry := range sourceEntries {
		nextDestDir := filepath.Join(destDir, fmt.Sprintf("%d", nextDestSubDirNum+i))

		err = copy.Copy(filepath.Join(sourceDir, entry.Name()), nextDestDir)
		if err != nil {
			return err
		}
	}

	return nil
}
