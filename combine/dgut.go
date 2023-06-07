package combine

import (
	"io"

	"github.com/wtsi-ssg/wrstat/v4/dgut"
)

const dgutStoreBatchSize = 10000
const dgutSumCols = 4
const numSummaryColumnsDGUT = 4

// DgutFiles merges the pre-sorted dgut files, summing consecutive lines with
// the same first 4 columns, and outputs the results to an embedded database.
func DgutFiles(inputs []string, outputDir string) error {
	sortMergeOutput, cleanup, err := MergeSortedFiles(inputs)
	if err != nil {
		return err
	}

	db := dgut.NewDB(outputDir)
	reader, writer := io.Pipe()
	errCh := make(chan error, 1)

	go func() {
		errs := db.Store(reader, dgutStoreBatchSize)

		if errs != nil {
			reader.Close()
		}

		errCh <- errs
	}()

	if err = MergeSummaryLines(sortMergeOutput, dgutSumCols,
		numSummaryColumnsDGUT, sumCountAndSizeAndKeepOldestAtime, writer); err != nil {
		return err
	}

	if err = writer.Close(); err != nil {
		return err
	}

	err = <-errCh
	if err != nil {
		return err
	}

	return cleanup()
}
