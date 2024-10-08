package merge

import (
	"errors"
	"fmt"
	"os"

	"github.com/wtsi-ssg/wrstat/v5/internal/fs"
)

const SentinelComplete = "combine.complete"

// Merge finds the latest completed run in the source dir and copies it,
// preserving timestamps, into the destination.
//
// When the removeOld param is set to true, the function will remove any runs
// older that the one that is copied.
func Merge(sourceDir, destDir string, removeOld bool) error {
	de, err := fs.FindLatestCombinedOutput(sourceDir, SentinelComplete)
	if errors.Is(err, fs.ErrNoDirEntryFound) {
		return nil
	} else if err != nil {
		return fmt.Errorf("failed to find database files in source dir: %w", err)
	}

	if err = fs.CopyPreservingTimestamp(de, destDir); err != nil {
		return fmt.Errorf("failed to copy files to new dest: %w", err)
	}

	if !removeOld {
		return nil
	}

	fi, err := os.Lstat(de)
	if err != nil {
		return fmt.Errorf("failed to stat latest directory (%s): %w", de, err)
	}

	if err := fs.RemoveFromDirWhenOlderThan(sourceDir, fi.ModTime()); err != nil {
		return fmt.Errorf("failed to remove old source files: %w", err)
	}

	return nil
}
