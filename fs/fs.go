package fs

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"

	"github.com/klauspost/pgzip"
)

type Error string

func (e Error) Error() string { return string(e) }

// FindFilePathsInDir finds files in the given dir that have basenames with the
// given suffix. Dies on error.
func FindFilePathsInDir(dir, suffix string) ([]string, error) {
	paths, err := filepath.Glob(fmt.Sprintf("%s/*%s", dir, suffix))
	if err != nil || len(paths) == 0 {
		return paths, Error("Error: could not find paths.")
	}

	return paths, nil
}

// CreateOutputFileInDir creates a file for writing in the given dir with the
// given basename. Dies on error.
func CreateOutputFileInDir(dir, basename string) (*os.File, error) {
	file, err := os.Create(filepath.Join(dir, basename))
	if err != nil {
		return file, err
	}

	return file, nil
}

// OpenFiles opens the given files for reading.
func OpenFiles(paths []string) ([]*os.File, error) {
	files := make([]*os.File, len(paths))

	for i, path := range paths {
		file, err := os.Open(path)
		if err != nil {
			return files, err
		}

		files[i] = file
	}

	return files, nil
}

func ReadCompressedFile(filePath string) (string, error) {
	actualFile, err := os.Open(filePath)
	if err != nil {
		return "", err
	}

	fileReader, err := pgzip.NewReader(actualFile)
	if err != nil {
		return "", err
	}

	defer fileReader.Close()

	fileScanner := bufio.NewScanner(fileReader)

	var fileContents string
	for fileScanner.Scan() {
		fileContents += fileScanner.Text() + "\n"
	}

	return fileContents, nil
}
