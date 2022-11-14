package combine

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/v3/fs"
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
			actualContent, err := fs.ReadCompressedFile(outputPath)
			So(err, ShouldBeNil)

			expectedContent := "KMace34\tkyle\ttest/dir/\t21\t27\n"
			So(actualContent, ShouldEqual, expectedContent)
		})
	})
}

// buildByUserGroupFiles builds six testing files, whereby each file contains
// the following tab-separated data:
//
// username group directory filecount filesize (for all files, the first 3 are
// the same and the last 2 are different),
//
// and the even number files belong to a different group than the odd number
// files.
func buildByUserGroupFiles(t *testing.T) ([]*os.File, *os.File, string) {
	t.Helper()

	paths := [6]string{"walk.1.byusergroup", "walk.2.byusergroup", "walk.3.byusergroup",
		"walk.4.byusergroup", "walk.5.byusergroup", "walk.6.byusergroup"}
	dir := t.TempDir()

	inputs := make([]*os.File, len(paths))

	for i, path := range paths {
		f, err := os.Create(filepath.Join(dir, path))
		if err != nil {
			t.Fatal(err)
		}

		if even(i) {
			fileID := os.Getuid()
			file2ID := os.Geteuid()
			fmt.Println(fileID)
			fmt.Println(file2ID)

			fileGroups, errs := os.Getgroups()
			if errs != nil {
				t.Fatal(errs)
			}

			err = os.Lchown(f.Name(), 1000, fileGroups[1])
			if err != nil {
				t.Fatal(err)
			}
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

func even(n int) bool {
	return n%2 == 0
}
