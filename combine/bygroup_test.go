package combine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestByGroupFiles(t *testing.T) {
	Convey("Given bygroup files and an output", t, func() {
		inputs, output, outputPath := buildByGroupFiles(t)

		err := MergeByGroupFiles(inputs, output)
		So(err, ShouldBeNil)

		Convey("You can merge and compress the bygroup files to the output", func() {
			_, err := os.Stat(outputPath)
			So(err, ShouldBeNil)
		})

		Convey("The proper content exists within the output file", func() {
			b, err := os.ReadFile(outputPath)
			So(err, ShouldBeNil)

			actualContent := string(b)
			So(err, ShouldBeNil)

			expectedContent := "kyle\tKMace34\t6\t9\n"
			So(actualContent, ShouldEqual, expectedContent)
		})
	})
}

func buildByGroupFiles(t *testing.T) ([]*os.File, *os.File, string) {
	t.Helper()

	paths := [3]string{"walk.1.bygroup", "walk.2.bygroup", "walk.3.bygroup"}
	dir := t.TempDir()

	inputs := make([]*os.File, len(paths))

	for i, path := range paths {
		f, err := os.Create(filepath.Join(dir, path))
		if err != nil {
			t.Fatal(err)
		}

		_, err = f.WriteString(fmt.Sprintf("%s\t%s\t%d\t%d\n", "kyle", "KMace34", i+1, i+2))
		if err != nil {
			t.Fatal(err)
		}

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
