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
	"io"
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/v3/fs"
)

// TestConcatenateMergeAndCompress tests the concat, merge, and compress
// functionality of the combine package.
func TestConcatenateMergeAndCompress(t *testing.T) {
	Convey("Given some inputs and an output", t, func() {
		maxLengthDir := "longString/longStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlong/StringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlongStringlong" //nolint:lll
		So(len(maxLengthDir), ShouldEqual, 4096)

		inputs, output, outputPath := buildCombineInputs(t,
			[]string{"path1", "path2", "path3", "path4", "path5", "path6"},
			[]string{"user1", "user2", "user3"},
			[]string{"group1", "group2", "group3"},
			[]string{"short/test/dir/", maxLengthDir})

		expectedConcatenatedOutput := fmt.Sprintf("user1\tgroup1\tshort/test/dir/\t1\t2\n"+
			"user1\tgroup1\tshort/test/dir/\t2\t3\n"+
			"user2\tgroup2\tshort/test/dir/\t3\t4\n"+
			"user2\tgroup2\t%s\t4\t5\n"+
			"user3\tgroup3\t%s\t5\t6\n"+
			"user3\tgroup3\t%s\t6\t7\n", maxLengthDir, maxLengthDir, maxLengthDir)

		expectedMergeOutput := fmt.Sprintf("user1\tgroup1\tshort/test/dir/\t1\t2\n"+
			"user1\tgroup1\tshort/test/dir/\t2\t3\n"+
			"user2\tgroup2\t%s\t4\t5\n"+
			"user2\tgroup2\tshort/test/dir/\t3\t4\n"+
			"user3\tgroup3\t%s\t5\t6\n"+
			"user3\tgroup3\t%s\t6\t7\n", maxLengthDir, maxLengthDir, maxLengthDir)

		expectedMergeLinesOutput := fmt.Sprintf("user1\tgroup1\tshort/test/dir/\t3\t5\n"+
			"user2\tgroup2\t%s\t4\t5\n"+
			"user2\tgroup2\tshort/test/dir/\t3\t4\n"+
			"user3\tgroup3\t%s\t11\t13\n", maxLengthDir, maxLengthDir)

		Convey("You can concatenate the inputs to the output", func() {
			err := Concatenate(inputs, output)
			So(err, ShouldBeNil)

			output.Close()

			b, err := os.ReadFile(outputPath)
			So(err, ShouldBeNil)
			So(string(b), ShouldEqual, expectedConcatenatedOutput)
		})

		Convey("You can concatenate the inputs to a compressed output", func() {
			compressor, closer, err := Compress(output)
			So(err, ShouldBeNil)

			err = Concatenate(inputs, compressor)
			So(err, ShouldBeNil)

			closer()

			b, err := os.ReadFile(outputPath)
			So(err, ShouldBeNil)
			So(string(b), ShouldNotEqual, expectedConcatenatedOutput)

			actualFileContents, err := fs.ReadCompressedFile(outputPath)
			So(err, ShouldBeNil)
			So(actualFileContents, ShouldEqual, expectedConcatenatedOutput)
		})

		Convey("You can concatenate the inputs to a compressed output in a single method call", func() {
			err := ConcatenateAndCompress(inputs, output)
			So(err, ShouldBeNil)

			b, err := os.ReadFile(outputPath)
			So(err, ShouldBeNil)
			So(string(b), ShouldNotEqual, expectedConcatenatedOutput)

			actualFileContents, err := fs.ReadCompressedFile(outputPath)
			So(err, ShouldBeNil)
			So(actualFileContents, ShouldEqual, expectedConcatenatedOutput)
		})

		Convey("You can merge the inputs to the output", func() {
			err := Merge(inputs, output, concatMerger)
			So(err, ShouldBeNil)

			b, err := os.ReadFile(outputPath)
			So(err, ShouldBeNil)
			So(string(b), ShouldEqual, expectedMergeOutput)
		})

		Convey("You can merge and compress the inputs to the output", func() {
			compressor, closer, err := Compress(output)
			So(err, ShouldBeNil)

			err = Merge(inputs, compressor, concatMerger)
			So(err, ShouldBeNil)

			closer()

			b, err := os.ReadFile(outputPath)
			So(err, ShouldBeNil)
			So(string(b), ShouldNotEqual, expectedMergeOutput)

			actualFileContents, err := fs.ReadCompressedFile(outputPath)
			So(err, ShouldBeNil)
			So(actualFileContents, ShouldEqual, expectedMergeOutput)
		})

		Convey("You can merge and compress the inputs to the output in a single call", func() {
			merger := concatMerger

			err := MergeAndCompress(inputs, output, merger)
			So(err, ShouldBeNil)

			b, err := os.ReadFile(outputPath)
			So(err, ShouldBeNil)
			So(string(b), ShouldNotEqual, expectedMergeOutput)

			actualFileContents, err := fs.ReadCompressedFile(outputPath)
			So(err, ShouldBeNil)
			So(actualFileContents, ShouldEqual, expectedMergeOutput)
		})

		Convey("You can merge the file contents to an output", func() {
			inputFiles := make([]string, len(inputs))
			for i, file := range inputs {
				inputFiles[i] = file.Name()
			}

			sortMergeOutput, cleanup, err := MergeSortedFiles(inputFiles)
			So(err, ShouldBeNil)

			err = MergeSummaryLines(sortMergeOutput, 3, 2, lineMerger, output)
			So(err, ShouldBeNil)

			err = cleanup()
			So(err, ShouldBeNil)

			b, err := os.ReadFile(outputPath)
			So(err, ShouldBeNil)

			So(string(b), ShouldEqual, expectedMergeLinesOutput)
		})

		Convey("You can merge the file contents to a compressed output", func() {
			inputFiles := make([]string, len(inputs))
			for i, file := range inputs {
				inputFiles[i] = file.Name()
			}

			sortMergeOutput, cleanup, err := MergeSortedFiles(inputFiles)
			So(err, ShouldBeNil)

			zw, closeOutput, err := Compress(output)
			So(err, ShouldBeNil)

			err = MergeSummaryLines(sortMergeOutput, 3, 2, lineMerger, zw)
			So(err, ShouldBeNil)

			closeOutput()

			err = cleanup()
			So(err, ShouldBeNil)

			b, err := os.ReadFile(outputPath)
			So(err, ShouldBeNil)
			So(b, ShouldNotEqual, expectedMergeLinesOutput)

			actualFileContents, err := fs.ReadCompressedFile(outputPath)
			So(err, ShouldBeNil)
			So(actualFileContents, ShouldEqual, expectedMergeLinesOutput)
		})
	})
}

// concatMerger specifies how a set of files is merged to an output.
func concatMerger(data io.ReadCloser, output io.Writer) error {
	_, err := io.Copy(output, data)

	return err
}

// lineMerger is a matchingSummaryLineMerger that, given cols 2, will sum
// the second to last element of a and b and store the result in a[penultimate],
// and likewise for the last element in a[last]. This corresponds to summing the
// file count and size columns of 2 lines in a by* file.
func lineMerger(cols int, a, b []string) {
	last := len(a) - (cols - 1)
	penultimate := last - 1

	a[penultimate] = addNumberStrings(a[penultimate], b[penultimate])
	a[last] = addNumberStrings(a[last], b[last])
}

// buildCombineInputs creates the combine files needed for testing,
// returning a list of the open files, the open output file and the output file
// path.
func buildCombineInputs(t *testing.T, paths, users, groups, dirs []string) ([]*os.File, *os.File, string) {
	t.Helper()
	dir := t.TempDir()

	inputs := createInputs(t, dir, paths, users, groups, dirs)

	outputPath := filepath.Join(dir, "output")

	fo, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("create error: %s", err)
	}

	return inputs, fo, outputPath
}

// createInputs creates the inputs needed for BuildCombineAndUserGroupInputs.
func createInputs(t *testing.T, testingDir string, paths, users, groups, dirs []string) []*os.File {
	t.Helper()

	inputs := make([]*os.File, len(paths))

	for i, path := range paths {
		f, err := os.Create(filepath.Join(testingDir, path))
		if err != nil {
			t.Fatal(err)
		}

		userAndGroupIndex := floor(float64(i) / 2)
		pathIndex := floor(float64(i) / 3)

		_, err = f.WriteString(fmt.Sprintf("%s\t%s\t%s\t%d\t%d\n", users[userAndGroupIndex],
			groups[userAndGroupIndex], dirs[pathIndex], i+1, i+2))
		if err != nil {
			t.Fatal(err)
		}

		inputs[i] = f

		f.Close()
	}

	filenames, err := fs.FindFilePathsInDir(testingDir, "")
	if err != nil {
		t.Fatal(err)
	}

	inputs, err = fs.OpenFiles(filenames)
	if err != nil {
		t.Fatal(err)
	}

	return inputs
}
