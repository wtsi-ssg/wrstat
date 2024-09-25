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
	"github.com/wtsi-ssg/wrstat/v5/internal/encode"
)

// TestStatFiles tests that the stat files concatenate and compress properly.
func TestStatFiles(t *testing.T) {
	Convey("Given stat files and an output", t, func() {
		dir, inputs, output, outputPath := buildStatFiles(t)

		Convey("You can concatenate and compress the stats files to the output", func() {
			err := StatFiles(inputs, output)
			So(err, ShouldBeNil)

			_, err = os.Stat(outputPath)
			So(err, ShouldBeNil)

			Convey("The proper content exists within the output file", func() {
				actualContent, err := fs.ReadCompressedFile(outputPath)
				So(err, ShouldBeNil)

				encodedDir := encode.Base64Encode(dir)

				expectedOutput := fmt.Sprintf(
					"%s\t5\t345\t152\t217434\t82183\t147\t'f'\t3\t7\t28472\t\n"+
						"%s\t6\t345\t152\t652302\t246549\t441\t'f'\t4\t7\t28472\t\n"+
						"%s\t7\t345\t152\t1087170\t410915\t735\t'f'\t5\t7\t28472\t\n", encodedDir, encodedDir, encodedDir)
				So(actualContent, ShouldEqual, expectedOutput)
			})
		})
	})
}

// buildStatFiles builds .stats files for testing.
func buildStatFiles(t *testing.T) (string, []*os.File, *os.File, string) {
	t.Helper()

	paths := [3]string{"walk.1.stats", "walk.2.stats", "walk.3.stats"}
	dir := t.TempDir()

	inputs := make([]*os.File, len(paths))

	for i, path := range paths {
		f, err := os.Create(filepath.Join(dir, path))
		if err != nil {
			t.Fatal(err)
		}

		_, err = f.WriteString(fmt.Sprintf(
			"%s\t%d\t%d\t%d\t%d\t%d\t%d\t%q\t%d\t%d\t%d\t\n",
			encode.Base64Encode(dir),
			5+i,
			345,
			152,
			217434*(i+i+1),
			82183*(i+i+1),
			147*(i+i+1),
			'f',
			3+i,
			7,
			28472))

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

	outputPath := filepath.Join(dir, "combine.stats.gz")

	fileOutput, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("create error: %s", err)
	}

	return dir, inputs, fileOutput, outputPath
}
