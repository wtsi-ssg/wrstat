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

		Convey("You can merge and compress the bygroup files to the output", func() {
			err := MergeByGroupFiles(inputs, output)
			So(err, ShouldBeNil)

			_, err = os.Stat(outputPath)
			So(err, ShouldBeNil)

			Convey("And the proper content exists within the output file", func() {
				b, err := os.ReadFile(outputPath)
				So(err, ShouldBeNil)

				actualContent := string(b)
				So(err, ShouldBeNil)

				expectedContent := "kyle\tKMace34\t21\t27\n"
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
	dir := t.TempDir()

	inputs := make([]*os.File, len(paths))

	for i, path := range paths {
		f, err := os.Create(filepath.Join(dir, path))
		if err != nil {
			t.Fatal(err)
		}

		if even(i) {
			fileID := os.Getuid()

			fileGroups, errs := os.Getgroups()
			if errs != nil {
				t.Fatal(errs)
			}

			err = os.Lchown(f.Name(), fileID, fileGroups[1])
			if err != nil {
				t.Fatal(err)
			}
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
