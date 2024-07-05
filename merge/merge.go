package merge

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"

	internaldb "github.com/wtsi-ssg/wrstat/v4/internal/db"
	ifs "github.com/wtsi-ssg/wrstat/v4/internal/fs"
)

const (
	reloadGrace = 5 * time.Minute
	watchFile   = "combine.log.gz"
)

func Merge(sourceDir, destDir string, removeOld bool) error {
	de, err := FindLatestCombinedOutputOlderThan(sourceDir, reloadGrace)
	if err != nil {
		return fmt.Errorf("failed to find database files in source dir: %w", err)
	}

	if err = copyPreservingTimestamp(de, destDir); err != nil {
		return fmt.Errorf("failed to copy files to new dest: %w", err)
	}

	if !removeOld {
		return nil
	}

	fi, err := os.Lstat(de)
	if err != nil {
		return fmt.Errorf("failed to stat latest directory (%s): %w", de, err)
	}

	if err := removeFromDirWhenOlderThan(sourceDir, fi.ModTime()); err != nil {
		return fmt.Errorf("failed to remove old source files: %w", err)
	}

	return nil
}

type pathTime struct {
	path    string
	modtime time.Time
}

// FindLatestCombinedOutputOlderThan finds the latest entry in dir and returns its path.
func FindLatestCombinedOutputOlderThan(dir string, minAge time.Duration) (string, error) {
	for {
		files, err := filepath.Glob(filepath.Join(dir, "*", "*", "*", watchFile))
		if err != nil {
			return "", err
		}

		if len(files) == 0 {
			return "", ifs.ErrNoDirEntryFound
		}

		de, err := filesToLatestPathTime(files)
		if err != nil {
			return "", err
		}

		diff := de.modtime.Sub(time.Now().Add(-minAge))

		if diff < 0 {
			return filepath.Dir(filepath.Dir(filepath.Dir(de.path))), nil
		}

		time.Sleep(diff)
	}
}

func filesToLatestPathTime(files []string) (pathTime, error) {
	des := make([]pathTime, len(files))

	for n, file := range files {
		fi, err := os.Lstat(file)
		if err != nil {
			return pathTime{}, err
		}

		des[n] = pathTime{
			path:    file,
			modtime: fi.ModTime(),
		}
	}

	sort.Slice(des, func(i, j int) bool {
		return des[i].modtime.After(des[j].modtime)
	})

	return des[0], nil
}

func copyPreservingTimestamp(source, dest string) error {
	fi, err := os.Lstat(source)
	if err != nil {
		return err
	}

	t := fi.ModTime()

	if fi.IsDir() {
		err = copyDirectoryPreservingTimestamp(source, dest)
	} else {
		err = copyFile(source, dest)
	}

	if err != nil {
		return err
	}

	return os.Chtimes(dest, t, t)
}

func copyDirectoryPreservingTimestamp(source, dest string) error {
	if err := os.MkdirAll(dest, internaldb.DirPerms); err != nil {
		return err
	}

	matches, err := filepath.Glob(source + "/*")
	if err != nil {
		return err
	}

	for _, match := range matches {
		if err := copyPreservingTimestamp(match, filepath.Join(dest, filepath.Base(match))); err != nil {
			return err
		}
	}

	return nil
}

func copyFile(source, dest string) (err error) {
	var f, d *os.File

	if f, err = os.Open(source); err != nil {
		return err
	}

	defer f.Close()

	if d, err = os.Create(dest); err != nil {
		return err
	}

	defer func() {
		if errr := d.Close(); err == nil {
			err = errr
		}
	}()

	_, err = io.Copy(d, f)

	return err
}

func removeFromDirWhenOlderThan(dir string, before time.Time) error {
	matches, err := filepath.Glob(dir + "/*")
	if err != nil {
		return err
	}

	for _, match := range matches {
		fi, err := os.Lstat(match)
		if err != nil {
			return err
		} else if !fi.ModTime().Before(before) {
			continue
		}

		if err := os.RemoveAll(match); err != nil {
			return err
		}
	}

	return nil
}
