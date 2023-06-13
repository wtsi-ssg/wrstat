package basedirs

import (
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/moby/sys/mountinfo"
	bolt "go.etcd.io/bbolt"
)

var (
	ErrInvalidBasePath  = errors.New("invalid base path")
	ErrNoBaseDirHistory = errors.New("no base dir history found")
)

type History struct {
	Date        time.Time
	UsageSize   uint64
	QuotaSize   uint64
	UsageInodes uint64
	QuotaInodes uint64
}

func (b *BaseDirReader) History(gid uint32, path string) ([]History, error) {
	mp := b.mountPoints.prefixOf(path)
	if mp == "" {
		return nil, ErrInvalidBasePath
	}

	var history []History

	if err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(groupHistoricalBucket))
		key := []byte(strconv.FormatUint(uint64(gid), 10) + bucketKeySeparator + mp)

		data := bucket.Get(key)
		if data == nil {
			return ErrNoBaseDirHistory
		}

		return b.decodeFromBytes(data, &history)
	}); err != nil {
		return nil, err
	}

	return history, nil
}

type mountPoints []string

func getMountPoints() (mountPoints, error) {
	mounts, err := mountinfo.GetMounts(nil)
	if err != nil {
		return nil, err
	}

	mountList := make(mountPoints, len(mounts))

	for n, mp := range mounts {
		if !strings.HasSuffix(mp.Mountpoint, "/") {
			mp.Mountpoint += "/"
		}

		mountList[n] = mp.Mountpoint
	}

	sort.Slice(mountList, func(i, j int) bool {
		return len(mountList[i]) > len(mountList[j])
	})

	return mountList, nil
}

func (m mountPoints) prefixOf(basedir string) string {
	for _, mount := range m {
		if strings.HasPrefix(basedir, mount) {
			return mount
		}
	}

	return ""
}
