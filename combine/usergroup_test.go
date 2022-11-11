package combine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestByUserGroupFiles(t *testing.T) {
	Convey("Given byusergroup files and an output", t, func() {
		inputs, output, outputPath := buildByUserGroupFiles(t)

		err := MergeUserGroupFiles(inputs, output)
		So(err, ShouldBeNil)

		Convey("You can merge and compress the byusergroup files to the output", func() {
			_, err := os.Stat(outputPath)
			So(err, ShouldBeNil)
		})

		Convey("The proper content exists within the output file", func() {
			actualContent, err := ReadCompressedFile(outputPath)
			So(err, ShouldBeNil)

			expectedContent := "KMace34\tkyle\ttest/dir/\t6\t9\n"
			So(actualContent, ShouldEqual, expectedContent)
		})
	})
}

func buildByUserGroupFiles(t *testing.T) ([]*os.File, *os.File, string) {
	t.Helper()

	paths := [3]string{"walk.1.byusergroup", "walk.2.byusergroup", "walk.3.byusergroup"}
	dir := t.TempDir()

	inputs := make([]*os.File, len(paths))

	for i, path := range paths {
		f, err := os.Create(filepath.Join(dir, path))
		if err != nil {
			t.Fatal(err)
		}

		_, err = f.WriteString(fmt.Sprintf("%s\t%s\t%s\t%d\t%d", "KMace34", "kyle", "test/dir/", i+1, i+2))
		if err != nil {
			t.Fatal(err)
		}

		inputs[i] = f

		f.Close()
	}

	outputPath := filepath.Join(dir, "combine.byusergroup.gz")

	fileOutput, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("create error: %s", err)
	}

	return inputs, fileOutput, outputPath
}
