package fs

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"

	"github.com/klauspost/pgzip"
)

// filePerms used to declare file mode permissions when making a new directory.
const filePerms = 448

const maxSize = 100000000

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
	return os.Create(filepath.Join(dir, basename))
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

// ReadCompressedFile takes the path of a compressed file, decompresses it, and
// returns the contents.
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
	fileScanner.Buffer([]byte{}, maxSize)

	var fileContents string
	for fileScanner.Scan() {
		fileContents += fileScanner.Text() + "\n"
	}

	return fileContents, nil
}

// RemoveAndCreateDir creates a dgut output dir in the given dir.
// Returns the path to the created directory. If it already existed, will delete
// it first, since we can't store to a pre-existing db.
func RemoveAndCreateDir(dir string) (string, error) {
	os.RemoveAll(dir)

	err := os.MkdirAll(dir, filePerms)
	if err != nil {
		return "", err
	}

	return dir, nil
}

// FindOpenAndCreate takes an input and output directory, each with their own
// file suffix. Filepaths are located in the input directory, using the input
// suffix, an output file is created in the output directory, using the output
// suffix, and the two are then both returned.
func FindOpenAndCreate(inputDir, outputDir, inputDirSuffix, outputDirSuffix string) ([]*os.File, *os.File, error) {
	paths, err := FindFilePathsInDir(inputDir, inputDirSuffix)
	if err != nil {
		return nil, nil, err
	}

	inputFiles, err := OpenFiles(paths)
	if err != nil {
		return nil, nil, err
	}

	output, err := CreateOutputFileInDir(outputDir, outputDirSuffix)
	if err != nil {
		return nil, nil, err
	}

	return inputFiles, output, nil
}
