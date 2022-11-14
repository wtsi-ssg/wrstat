package combine

import (
	"io"
	"os"
)

const numSummaryColumns = 2
const groupSumCols = 2

func MergeByGroupFiles(inputs []*os.File, output *os.File) error {
	return Merge(inputs, output, mergeGroupStreamToFile)
}

// mergeGroupStreamToFile merges pre-sorted (pre-merged) group data
// (eg. from a `sort -m` of .bygroup files), summing consecutive lines with
// the same first 2 columns, and outputting the results.
func mergeGroupStreamToFile(data io.ReadCloser, output io.Writer) error {
	if err := MergeSummaryLines(data, groupSumCols, numSummaryColumns, sumCountAndSize, output); err != nil {
		return err
	}

	return nil
}
