package combine

import (
	b64 "encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/v3/fs"
)

func TestStatFiles(t *testing.T) {
	Convey("Given stat files and an output", t, func() {
		dir, inputs, output, outputPath := buildStatFiles(t)

		err := concatenateAndCompressStatsFiles(inputs, output)
		So(err, ShouldBeNil)

		Convey("You can concatenate and compress the stats files to the output", func() {
			_, err := os.Stat(outputPath)
			So(err, ShouldBeNil)
		})

		Convey("The proper content exists within the output file", func() {
			actualContent, err := fs.ReadCompressedFile(outputPath)
			So(err, ShouldBeNil)

			encodedDir := b64.StdEncoding.EncodeToString([]byte(dir))

			expectedContent := fmt.Sprintf(
				"%s\t5\t345\t152\t217434\t82183\t147\t'f'\t3\t7\t28472\t\n"+
					"%s\t6\t345\t152\t652302\t246549\t441\t'f'\t4\t7\t28472\t\n"+
					"%s\t7\t345\t152\t1087170\t410915\t735\t'f'\t5\t7\t28472\t\n", encodedDir, encodedDir, encodedDir)
			So(actualContent, ShouldEqual, expectedContent)
		})
	})
}

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
			b64.StdEncoding.EncodeToString([]byte(dir)),
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
