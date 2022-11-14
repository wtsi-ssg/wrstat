package combine

import "os"

// concatenateAndCompressStatsFiles finds and conatenates the stats files and
// compresses the output.
func concatenateAndCompressStatsFiles(inputs []*os.File, output *os.File) error {
	if err := ConcatenateAndCompress(inputs, output); err != nil {
		return err
	}

	return nil
}
