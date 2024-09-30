package combine

import (
	"io"

	"github.com/wtsi-ssg/wrstat/v5/dgut"
)

const dgutStoreBatchSize = 10000
const dgutSumCols = 4
const numSummaryColumnsDGUT = 4

// DgutFiles merges the pre-sorted dgut files, summing consecutive lines with
// the same first 4 columns, and outputs the results to an embedded database.
func DgutFiles(inputs []string, outputDir string) (err error) {
	sortMergeOutput, cleanup, err := MergeSortedFiles(inputs)
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)

	defer func() {
		select {
		case e := <-errCh:
			if e != nil {
				err = e
			}
		default:
		}
	}()

	return processDgutFiles(outputDir, sortMergeOutput, cleanup, errCh)
}

func processDgutFiles(outputDir string, sortMergeOutput io.ReadCloser, cleanup func() error, errCh chan error) error {
	db := dgut.NewDB(outputDir)
	reader, writer := io.Pipe()

	go dgutStore(db, reader, errCh)

	if err := MergeSummaryLines(
		sortMergeOutput,
		dgutSumCols,
		numSummaryColumnsDGUT,
		sumCountAndSizeAndKeepOldestAtime,
		writer,
	); err != nil {
		return err
	}

	if err := writer.Close(); err != nil {
		return err
	}

	err := <-errCh
	if err != nil {
		return err
	}

	return cleanup()
}

func dgutStore(db *dgut.DB, reader io.ReadCloser, errCh chan error) {
	errs := db.Store(reader, dgutStoreBatchSize)

	if errs != nil {
		reader.Close()
	}

	errCh <- errs
}
