package combine

import (
	"fmt"
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
			So(string(b), ShouldEqual, "KMace34\tkyle\ttest/dir/\t1\t2\nKMace34\tkyle\ttest/dir/\t2\t3\n")
		})

		Convey("You can concatenate the inputs to compressed output", func() {
			compressor, closer, err := Compress(output)
			So(err, ShouldBeNil)

			err = Concatenate(inputs, compressor)
			So(err, ShouldBeNil)

			closer()

			b, err := os.ReadFile(outputPath)
			So(err, ShouldBeNil)
			So(string(b), ShouldNotEqual, "KMace34\tkyle\ttest/dir/\t1\t2\nKMace34\tkyle\ttest/dir/\t2\t3\n")

			actualFileContents, err := fs.ReadCompressedFile(outputPath)
			So(err, ShouldBeNil)
			So(actualFileContents, ShouldEqual, "KMace34\tkyle\ttest/dir/\t1\t2\nKMace34\tkyle\ttest/dir/\t2\t3\n")
		})

		Convey("You can concatenate the inputs to compressed output in a single method call", func() {
			err := ConcatenateAndCompress(inputs, output)
			So(err, ShouldBeNil)

			b, err := os.ReadFile(outputPath)
			So(err, ShouldBeNil)
			So(string(b), ShouldNotEqual, "KMace34\tkyle\ttest/dir/\t1\t2\nKMace34\tkyle\ttest/dir/\t2\t3\n")

			actualFileContents, err := fs.ReadCompressedFile(outputPath)
			So(err, ShouldBeNil)
			So(actualFileContents, ShouldEqual, "KMace34\tkyle\ttest/dir/\t1\t2\nKMace34\tkyle\ttest/dir/\t2\t3\n")
		})

		Convey("You can merge the inputs to the output", func() {
			merger := myMerger

			err := Merge(inputs, output, merger)
			So(err, ShouldBeNil)

			b, err := os.ReadFile(outputPath)
			So(err, ShouldBeNil)
			So(string(b), ShouldEqual, "KMace34\tkyle\ttest/dir/\t1\t2\nKMace34\tkyle\ttest/dir/\t2\t3\n")
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
			So(string(b), ShouldNotEqual, "KMace34\tkyle\ttest/dir/\t1\t2\nKMace34\tkyle\ttest/dir/\t2\t3\n")

			actualFileContents, err := fs.ReadCompressedFile(outputPath)
			So(err, ShouldBeNil)
			So(actualFileContents, ShouldEqual, "KMace34\tkyle\ttest/dir/\t1\t2\nKMace34\tkyle\ttest/dir/\t2\t3\n")
		})

		Convey("You can merge and compress the inputs to the output in a single call", func() {
			merger := myMerger

			err := MergeAndCompress(inputs, output, merger)
			So(err, ShouldBeNil)

			b, err := os.ReadFile(outputPath)
			So(err, ShouldBeNil)
			So(string(b), ShouldNotEqual, "KMace34\tkyle\ttest/dir/\t1\t2\nKMace34\tkyle\ttest/dir/\t2\t3\n")

			actualFileContents, err := fs.ReadCompressedFile(outputPath)
			So(err, ShouldBeNil)
			So(actualFileContents, ShouldEqual, "KMace34\tkyle\ttest/dir/\t1\t2\nKMace34\tkyle\ttest/dir/\t2\t3\n")
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

			So(string(b), ShouldEqual, "KMace34\tkyle\ttest/dir/\t3\t5\n")
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
			So(b, ShouldNotEqual, "KMace34\tkyle\ttest/dir/\t3\t5\n")

			actualFileContents, err := fs.ReadCompressedFile(outputPath)
			So(err, ShouldBeNil)
			So(actualFileContents, ShouldEqual, "KMace34\tkyle\ttest/dir/\t3\t5\n")
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
	paths := [2]string{"path1", "path2"}
	inputs := make([]*os.File, len(paths))

	for i, path := range paths {
		f, err := os.Create(filepath.Join(dir, path))
		if err != nil {
			t.Fatal(err)
		}

		_, err = f.WriteString(fmt.Sprintf("%s\t%s\t%s\t%d\t%d\n", "KMace34", "kyle", "test/dir/", i+1, i+2))
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

	outputPath := filepath.Join(dir, "output")

	fo, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("create error: %s", err)
	}

	return inputs, fo, outputPath
}
