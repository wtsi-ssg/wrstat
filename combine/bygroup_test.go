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
	"math"
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

// TestByGroupFiles tests that the .bygroup files properly merge to the output
// file.
func TestByGroupFiles(t *testing.T) {
	Convey("Given bygroup files and an output", t, func() {
		inputs, output, outputPath := buildByGroupFiles(t)

		Convey("You can merge the bygroup files to the output", func() {
			err := GroupFiles(inputs, output)
			So(err, ShouldBeNil)

			_, err = os.Stat(outputPath)
			So(err, ShouldBeNil)

			Convey("And the proper content exists within the output file", func() {
				b, err := os.ReadFile(outputPath)
				So(err, ShouldBeNil)

				actualContent := string(b)
				So(err, ShouldBeNil)

				expectedContent := "group1\tuser1\t3\t5\ngroup2\tuser2\t7\t9\ngroup3\tuser3\t11\t13\n"
				So(actualContent, ShouldEqual, expectedContent)
			})
		})
	})
}

// buildByGroupFiles builds six testing files, whereby each file contains
// the following tab-separated data:
//
// group username filecount filesize (for all files, the first 2 are
// the same and the last 2 are different),
//
// and the even number files belong to a different group than the odd number
// files.
func buildByGroupFiles(t *testing.T) ([]*os.File, *os.File, string) {
	t.Helper()

	paths := [6]string{"walk.1.bygroup", "walk.2.bygroup", "walk.3.bygroup",
		"walk.4.bygroup", "walk.5.bygroup", "walk.6.bygroup"}
	users, groups := [3]string{"user1", "user2", "user3"}, [3]string{"group1", "group2", "group3"}

	dir := t.TempDir()
	inputs := make([]*os.File, len(paths))

	for i, path := range paths {
		f, err := os.Create(filepath.Join(dir, path))
		So(err, ShouldBeNil)

		index := floor(float64(i) / 2)

		_, err = f.WriteString(fmt.Sprintf("%s\t%s\t%d\t%d\n", groups[index], users[index], i+1, i+2))
		So(err, ShouldBeNil)

		inputs[i] = f

		f.Close()
	}

	outputPath := filepath.Join(dir, "combine.bygroup")

	fileOutput, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("create error: %s", err)
	}

	return inputs, fileOutput, outputPath
}

// floor returns the floor (a round-down, e.g. floor(3.9) = 3) of a float x.
func floor(x float64) int {
	return int(math.Floor(x))
}
