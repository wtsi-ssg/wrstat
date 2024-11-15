package combine

import (
	"io"

	"github.com/wtsi-ssg/wrstat/v5/dguta"
)

const (
	dgutaStoreBatchSize    = 10000
	dgutaSumCols           = 5
	numSummaryColumnsDGUTA = 4
	dgutaAtimeColIndex     = 7
	dgutaMtimeColIndex     = 8
)

// DgutaFiles merges the pre-sorted dguta files, summing consecutive lines with
// the same first 5 columns, and outputs the results to an embedded database.
func DgutaFiles(inputs []string, outputDir string) (err error) {
	sortMergeOutput, err := MergeSortedFiles(inputs)
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

	return processDgutaFiles(outputDir, sortMergeOutput, errCh)
}

func processDgutaFiles(outputDir string, sortMergeOutput io.ReadCloser, errCh chan error) error {
	db := dguta.NewDB(outputDir)
	reader, writer := io.Pipe()

	go dgutaStore(db, reader, errCh)

	if err := MergeSummaryLines(
		sortMergeOutput,
		dgutaSumCols,
		numSummaryColumnsDGUTA,
		sumCountAndSizesAndKeepTimes,
		writer,
	); err != nil {
		return err
	}

	if err := writer.Close(); err != nil {
		return err
	}

	return <-errCh
}

func dgutaStore(db *dguta.DB, reader io.ReadCloser, errCh chan error) {
	errs := db.Store(reader, dgutaStoreBatchSize)

	if errs != nil {
		reader.Close()
	}

	errCh <- errs
}
