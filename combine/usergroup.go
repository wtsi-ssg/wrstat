package combine

import (
	"io"
	"os"
	"strconv"
)

func MergeUserGroupFiles(inputs []*os.File, output *os.File) error {
	return MergeAndCompress(inputs, output, mergeUserGroupStreamToCompressedFile)
}

// mergeUserGroupStreamToCompressedFile merges pre-sorted (pre-merged) usergroup
// data (eg. from a `sort -m` of .byusergroup files), summing consecutive lines
// with the first 3 columns, and outputting the results to a file, compressed.
func mergeUserGroupStreamToCompressedFile(data io.ReadCloser, output io.Writer) error {
	if err := MergeSummaryLines(data, 3, 2, sumCountAndSize, output); err != nil {
		return err
	}

	return nil
}

// sumCountAndSize is a matchingSummaryLineMerger that, given cols 2,  will sum
// the second to last element of a and b and store the result in a[penultimate],
// and likewise for the last element in a[last]. This corresponds to summing the
// file count and size columns of 2 lines in a by* file.
func sumCountAndSize(cols int, a, b []string) {
	last := len(a) - (cols - 1)
	penultimate := last - 1

	a[penultimate] = addNumberStrings(a[penultimate], b[penultimate])
	a[last] = addNumberStrings(a[last], b[last])
}

// addNumberStrings treats a and b as ints, adds them together, and returns the
// resulting int64 as a string.
func addNumberStrings(a, b string) string {
	return strconv.FormatInt(Atoi(a)+Atoi(b), 10)
}

// Atoi is like strconv.Atoi but returns an int64 and dies on error.
func Atoi(n string) int64 {
	i, _ := strconv.ParseInt(n, 10, 0)

	return i
}
