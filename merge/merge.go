package merge

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	internaldb "github.com/wtsi-ssg/wrstat/v4/internal/db"
	"github.com/wtsi-ssg/wrstat/v4/internal/fs"
)

const (
	reloadGrace      = 5 * time.Minute
	SentinelComplete = "combine.complete"
)

// Merge finds the latest completed run in the source dir and copies it,
// preserving timestamps, into the destination.
//
// When the removeOld param is set to true, the function will remove any runs
// older that the one that is copied.
func Merge(sourceDir, destDir string, removeOld bool) error {
	de, err := fs.FindLatestCombinedOutputOlderThan(sourceDir, SentinelComplete, reloadGrace)
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
