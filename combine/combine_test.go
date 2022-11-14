package combine

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/v3/fs"
)

// TestConcatenateMergeAndCompress tests the concat, merge, and compress
// functionality of the combine package.
func TestConcatenateMergeAndCompress(t *testing.T) {
	Convey("Given some inputs and an output", t, func() {
		inputs, output, outputPath := createInputsAndOutput(t)

		Convey("You can concatenate the inputs to the output", func() {
			err := Concatenate(inputs, output)
			So(err, ShouldBeNil)

			output.Close()

			b, err := os.ReadFile(outputPath)
			So(err, ShouldBeNil)
			So(string(b), ShouldEqual, "line from path1\nline from path2\n")
		})

		Convey("You can concatenate the inputs to compressed output", func() {
			compressor, closer, err := Compress(output)
			So(err, ShouldBeNil)

			err = Concatenate(inputs, compressor)
			So(err, ShouldBeNil)

			closer()

			b, err := os.ReadFile(outputPath)
			So(err, ShouldBeNil)
			So(string(b), ShouldNotEqual, "line from path1\nline from path2\n")

			actualFileContents, err := fs.ReadCompressedFile(outputPath)
			So(err, ShouldBeNil)
			So(actualFileContents, ShouldEqual, "line from path1\nline from path2\n")
		})

		Convey("You can concatenate the inputs to compressed output in a single method call", func() {
			err := ConcatenateAndCompress(inputs, output)
			So(err, ShouldBeNil)

			b, err := os.ReadFile(outputPath)
			So(err, ShouldBeNil)
			So(string(b), ShouldNotEqual, "line from path1\nline from path2\n")

			actualFileContents, err := fs.ReadCompressedFile(outputPath)
			So(err, ShouldBeNil)
			So(actualFileContents, ShouldEqual, "line from path1\nline from path2\n")
		})

		Convey("You can merge the inputs to the output", func() {
			merger := myMerger

			err := Merge(inputs, output, merger)
			So(err, ShouldBeNil)

			b, err := os.ReadFile(outputPath)
			So(err, ShouldBeNil)
			So(string(b), ShouldEqual, "line from path1\nline from path2\n")
		})

		Convey("You can merge and compress the inputs to the output", func() {
			merger := myMerger

			compressor, closer, err := Compress(output)
			So(err, ShouldBeNil)

			err = Merge(inputs, compressor, merger)
			So(err, ShouldBeNil)

			closer()

			b, err := os.ReadFile(outputPath)
			So(err, ShouldBeNil)
			So(string(b), ShouldNotEqual, "line from path1\nline from path2\n")

			actualFileContents, err := fs.ReadCompressedFile(outputPath)
			So(err, ShouldBeNil)
			So(actualFileContents, ShouldEqual, "line from path1\nline from path2\n")
		})

		Convey("You can merge and compress the inputs to the output in a single call", func() {
			merger := myMerger

			err := MergeAndCompress(inputs, output, merger)
			So(err, ShouldBeNil)

			b, err := os.ReadFile(outputPath)
			So(err, ShouldBeNil)
			So(string(b), ShouldNotEqual, "line from path1\nline from path2\n")

			actualFileContents, err := fs.ReadCompressedFile(outputPath)
			So(err, ShouldBeNil)
			So(actualFileContents, ShouldEqual, "line from path1\nline from path2\n")
		})

		Convey("MergeSummaryLines merges the file contents to output", func() {
			inputFiles := make([]string, len(inputs))
			for i, file := range inputs {
				inputFiles[i] = file.Name()
			}

			sortMergeOutput, cleanup, err := mergeSortedFiles(inputFiles)
			So(err, ShouldBeNil)

			err = MergeSummaryLines(sortMergeOutput, 3, 2, myLineMerger, output)
			So(err, ShouldBeNil)

			err = cleanup()
			So(err, ShouldBeNil)

			b, err := os.ReadFile(outputPath)
			So(err, ShouldBeNil)

			So(string(b), ShouldEqual, "line from path1\nline from path2\n")
		})

		Convey("MergeSummaryLines merges the file contents to a compressed output", func() {
			inputFiles := make([]string, len(inputs))
			for i, file := range inputs {
				inputFiles[i] = file.Name()
			}

			sortMergeOutput, cleanup, err := mergeSortedFiles(inputFiles)
			So(err, ShouldBeNil)

			zw, closeOutput, err := Compress(output)
			So(err, ShouldBeNil)

			err = MergeSummaryLines(sortMergeOutput, 3, 2, myLineMerger, zw)
			So(err, ShouldBeNil)

			closeOutput()

			err = cleanup()
			So(err, ShouldBeNil)

			b, err := os.ReadFile(outputPath)
			So(err, ShouldBeNil)
			So(b, ShouldNotEqual, "line from path1\nline from path2\n")

			actualFileContents, err := fs.ReadCompressedFile(outputPath)
			So(err, ShouldBeNil)
			So(actualFileContents, ShouldEqual, "line from path1\nline from path2\n")
		})
	})
}

// myMerger specifies how a set of files is merged to an output.
func myMerger(data io.ReadCloser, output io.Writer) error {
	if _, err := io.Copy(output, data); err != nil {
		return err
	}

	return nil
}

// myLineMerger is a matchingSummaryLineMerger that, given cols 2,  will sum
// the second to last element of a and b and store the result in a[penultimate],
// and likewise for the last element in a[last]. This corresponds to summing the
// file count and size columns of 2 lines in a by* file.
func myLineMerger(cols int, a, b []string) {
	last := len(a) - (cols - 1)
	penultimate := last - 1

	a[penultimate] = AddNumberStrings(a[penultimate], b[penultimate])
	a[last] = AddNumberStrings(a[last], b[last])
}

// AddNumberStrings treats a and b as ints, adds them together, and returns the
// resulting int64 as a string.
func AddNumberStrings(a, b string) string {
	return strconv.FormatInt(Atoi(a)+Atoi(b), 10)
}

func createInputsAndOutput(t *testing.T) ([]*os.File, *os.File, string) {
	dir := t.TempDir()
	input1Path := filepath.Join(dir, "path1")
	input2Path := filepath.Join(dir, "path2")

	f1, err := os.Create(input1Path)
	if err != nil {
		t.Fatalf("create error: %s", err)
	}

	f2, err := os.Create(input2Path)
	if err != nil {
		t.Fatalf("create error: %s", err)
	}

	_, err = f1.WriteString("line from path1\n")
	So(err, ShouldBeNil)

	_, err = f2.WriteString("line from path2\n")
	So(err, ShouldBeNil)

	f1.Close()
	f2.Close()

	f1, err = os.Open(input1Path)
	if err != nil {
		t.Fatalf("create error: %s", err)
	}

	f2, err = os.Open(input2Path)
	if err != nil {
		t.Fatalf("create error: %s", err)
	}

	outputPath := filepath.Join(dir, "output")

	fo, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("create error: %s", err)
	}

	return []*os.File{f1, f2}, fo, outputPath
}
