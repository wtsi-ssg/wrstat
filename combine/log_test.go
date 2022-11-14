package combine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/fs"
)

func TestLogFiles(t *testing.T) {
	Convey("Given byusergroup files and an output", t, func() {
		inputs, output, outputPath := buildLogFiles(t)

		err := mergeLogAndCompress(inputs, output)
		So(err, ShouldBeNil)

		Convey("You can merge and compress the byusergroup files to the output", func() {
			_, err := os.Stat(outputPath)
			So(err, ShouldBeNil)
		})

		Convey("The proper content exists within the output file", func() {
			actualContent, err := fs.ReadCompressedFile(outputPath)
			So(err, ShouldBeNil)

			expectedContent := "This is line number0\nThis is line number1\nThis is line number2\nThis is line number3\nThis is line number4\nThis is line number5\n"
			So(actualContent, ShouldEqual, expectedContent)
		})
	})
}

// buildLogFiles builds six testing files, whereby each file contains a line
// that reads, 'This is line number n', where n is the index of the for loop.
// The even number files belong to a different group than the odd number files.
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

		_, err = f.WriteString(fmt.Sprintf("This is line number%d\n", i))
		if err != nil {
			t.Fatal(err)
		}

		inputs[i] = f

		f.Close()
	}

	outputPath := filepath.Join(dir, "combine.log.gz")

	fileOutput, err := os.Create(outputPath)
	if err != nil {
		t.Fatalf("create error: %s", err)
	}

	return inputs, fileOutput, outputPath
}
