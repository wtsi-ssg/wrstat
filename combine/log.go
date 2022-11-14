package combine

import (
	"io"
	"os"
)

// mergeLogAndCompress merges the inputs and stores in the output, compressed.
func mergeLogAndCompress(inputs []*os.File, output *os.File) error {
	/*inputFiles, err := fs.OpenFiles(inputs)
	if err != nil {
		return err
	}*/

	return MergeAndCompress(inputs, output, mergeLogStreamToCompressedFile)
}

// mergeLogStreamToCompressedFile combines log data, outputting the results to a
// file, compressed.
func mergeLogStreamToCompressedFile(data io.ReadCloser, output io.Writer) error {
	if _, err := io.Copy(output, data); err != nil {
		return err
	}

	return nil
}
