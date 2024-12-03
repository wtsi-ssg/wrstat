package combine

import (
	"io"
	"os"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMergeSortedFiles(t *testing.T) {
	Convey("", t, func() {
		for _, test := range [...]struct {
			Inputs            []string
			Output            string
			UnquoteComparison bool
		}{
			{
				Inputs: []string{"abc\ndef", "abd\ncba"},
				Output: "abc\nabd\ncba\ndef\n",
			},
			{
				Inputs: []string{"\"abc\"\n\"def\"", "\"abd\"\n\"cba\""},
				Output: "\"abc\"\n\"abd\"\n\"cba\"\n\"def\"\n",
			},
			{
				Inputs:            []string{"\"abc\"\n\"def\"", "\"abd\"\n\"cba\""},
				Output:            "\"abc\"\n\"abd\"\n\"cba\"\n\"def\"\n",
				UnquoteComparison: true,
			},
			{
				Inputs:            []string{"\"ab\\nc\"\n\"def\"", "\"ab d\"\n\"cba\""},
				Output:            "\"ab\\nc\"\n\"ab d\"\n\"cba\"\n\"def\"\n",
				UnquoteComparison: true,
			},
			{
				Inputs: []string{"\"ab\\nc\"\n\"def\"", "\"ab d\"\n\"cba\""},
				Output: "\"ab d\"\n\"ab\\nc\"\n\"cba\"\n\"def\"\n",
			},
			{
				Inputs: []string{"a\nb\nc", "d\ne\nf\ng", "h"},
				Output: "a\nb\nc\nd\ne\nf\ng\nh\n",
			},
			{
				Inputs: []string{
					"\"/a/b/c/d\"\t3\t2\t1\t3\n\"/a/b/c/\"\t0\t10\t2\t3",
					"\"/a/b/c/dz\"\t3\t2\t1\t3\n\"/a/b/cz/\"\t0\t10\t2\t3",
				},
				Output: "\"/a/b/c/d\"\t3\t2\t1\t3\n\"/a/b/c/\"\t0\t10\t2\t3\n" +
					"\"/a/b/c/dz\"\t3\t2\t1\t3\n\"/a/b/cz/\"\t0\t10\t2\t3\n",
				UnquoteComparison: true,
			},
		} {
			files := make([]*os.File, len(test.Inputs))

			for n, input := range test.Inputs {
				r, w, err := os.Pipe()
				So(err, ShouldBeNil)

				files[n] = r

				go func() {
					w.WriteString(input) //nolint:errcheck
					w.Close()
				}()
			}

			var output strings.Builder

			r, err := MergeSortedFiles(files, test.UnquoteComparison)
			So(err, ShouldBeNil)

			_, err = io.Copy(&output, r)
			So(err, ShouldBeNil)
			So(output.String(), ShouldEqual, test.Output)
		}
	})
}
