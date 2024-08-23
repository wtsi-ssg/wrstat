/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * 		   Kyle Mace  <km34@sanger.ac.uk>
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

package combine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/v5/fs"
)

// TestLogFiles tests that the log files merge and compress properly.
func TestLogFiles(t *testing.T) {
	Convey("Given log files and an output", t, func() {
		inputs, output, outputPath := buildLogFiles(t)

		Convey("you can merge and compress the log files to the output", func() {
			err := LogFiles(inputs, output)
			So(err, ShouldBeNil)

			_, err = os.Stat(outputPath)
			So(err, ShouldBeNil)

			Convey("and the proper content exists within the output file", func() {
				actualContent, err := fs.ReadCompressedFile(outputPath)
				So(err, ShouldBeNil)

				expectedContent := "This is line number0\nThis is line number1\nThis is line number2\n" +
					"This is line number3\nThis is line number4\nThis is line number5\n"
				So(actualContent, ShouldEqual, expectedContent)
			})
		})
	})
}

// buildLogFiles builds six testing files, whereby each file contains a line
// that reads, 'This is line number n', where n is the index of the for loop.
func buildLogFiles(t *testing.T) ([]*os.File, *os.File, string) {
	t.Helper()

	paths := [6]string{"walk.1.log", "walk.2.log", "walk.3.log",
		"walk.4.log", "walk.5.log", "walk.6.log"}
	dir := t.TempDir()

	inputs := make([]*os.File, len(paths))

	for i, path := range paths {
		f, err := os.Create(filepath.Join(dir, path))
		if err != nil {
			t.Fatal(err)
		}

		_, err = f.WriteString(fmt.Sprintf("This is line number%d\n", i))
		if err != nil {
			t.Fatal(err)
		}

		inputs[i] = f

		f.Close()
	}

	filenames, err := fs.FindFilePathsInDir(dir, "")
	if err != nil {
		t.Fatal(err)
	}

	inputs, err = fs.OpenFiles(filenames)
	if err != nil {
		t.Fatal(err)
	}

	outputPath := filepath.Join(dir, "combine.log.gz")

	fileOutput, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("create error: %s", err)
	}

	return inputs, fileOutput, outputPath
}
