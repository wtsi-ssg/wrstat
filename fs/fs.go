package fs

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"

	"github.com/klauspost/pgzip"
)

// filePerms used to declare file mode permissions when making a new directory.
const filePerms = 0770

// bufferLength describes the amount of content scanned when decompressing.
// Given that the default MaxScanTokenSize is 65536, and we may get several
// concatenated lines that are each over 65536 chars in length, we multiply this
// by 10 to be safe.
const scanBufferSize = 10 * bufio.MaxScanTokenSize

type Error string

func (e Error) Error() string { return string(e) }

// FindFilePathsInDir finds files in the given dir that have basenames with the
// given suffix.
func FindFilePathsInDir(dir, suffix string) ([]string, error) {
	paths, err := filepath.Glob(fmt.Sprintf("%s/*%s", dir, suffix))
	if err != nil {
		return paths, err
	}

	if len(paths) == 0 {
		return paths, Error("Error: could not find paths.")
	}

	return paths, nil
}

// CreateOutputFileInDir creates a file for writing in the given dir with the
// given basename.
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
	fileScanner.Buffer([]byte{}, scanBufferSize)

	var fileContents string
	for fileScanner.Scan() {
		fileContents += fileScanner.Text() + "\n"
	}

	return fileContents, nil
}

// RemoveAndCreateDir creates the given directory, deleting it first if it
// already exists.
func RemoveAndCreateDir(dir string) error {
	os.RemoveAll(dir)

	err := os.MkdirAll(dir, filePerms)

	return err
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

// DirValid checks if the directory is valid: is absolute and exists.
func DirValid(dir string) error {
	dir, err := filepath.Abs(dir)
	if err != nil {
		return err
	}

	_, err = os.Stat(dir)

	return err
}
