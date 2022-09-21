/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 *         Kyle Mace <km34@sanger.ac.uk>
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
	"os"
	"path/filepath"
)

type Error string

func (e Error) Error() string { return string(e) }

const errBadDir = Error("srcDir or destDir was nonsense!")

const modePermUser = 0700

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
	}

	return err
}

/*func expeditedMove() {
	directoryMap := map[string]string{
		"combine.stats.gz": "stats.gz",
		"combine.byusergroup.gz": "byusergroup.gz",
		"combine.bygroup": "bygroup",
		"combine.log.gz": "logs.gz"}

	for fileBasename, suffix := range directoryMap {
		if err := findAndMoveOutputs(srcDir, destDir, destDirInfo, date,
			fileBasename, suffix); err != nil {
			return err
		}
	}
}

func findAndMoveOutputs(sourceDir, destDir string, destDirInfo fs.FileInfo,
	date, inputSuffix, outputSuffix string){

}*/

// Checks if the directory is valid; exists or not

func dirValid(addr string) error {
	addr, err := filepath.Abs(addr)
	if err != nil {
		return err
	}
	// Just return nil here instead?
	_, err = os.Stat(addr)

	return err
}
