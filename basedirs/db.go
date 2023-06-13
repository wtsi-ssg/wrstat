/*******************************************************************************
 * Copyright (c) 2022, 2023 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
 *   Michael Woolnough <mw31@sanger.ac.uk>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

package basedirs

import (
	"errors"
	"strconv"
	"time"

	"github.com/ugorji/go/codec"
	"github.com/wtsi-ssg/wrstat/v4/dgut"
	bolt "go.etcd.io/bbolt"
)

const dbOpenMode = 0600
const bucketKeySeparator = "-"

const (
	groupUsageBucket      = "groupUsage"
	groupHistoricalBucket = "groupHistorical"
	groupSubDirsBucket    = "groupSubDirs"
	userUsageBucket       = "userUsage"
	userSubDirsBucket     = "userSubDirs"
)

// Usage holds information summarising usage by a particular GID/UID-BaseDir.
type Usage struct {
	GID         uint32
	UID         uint32
	BaseDir     string
	UsageSize   uint64
	QuotaSize   uint64
	UsageInodes uint64
	QuotaInodes uint64
	Mtime       time.Time
}

// CreateDatabase creates a database containing usage information for each of
// our groups and users by calculated base directory.
//
// Provide a time that will be used as the date when appending to the historical
// data.
func (b *BaseDirs) CreateDatabase(historyDate time.Time) error {
	db, err := bolt.Open(b.dir, dbOpenMode, &bolt.Options{
		NoFreelistSync: true,
		NoGrowSync:     true,
		FreelistType:   bolt.FreelistMapType,
	})
	if err != nil {
		return err
	}

	gids, uids, err := getAllGIDsandUIDsInTree(b.tree)
	if err != nil {
		return err
	}

	err = db.Update(b.updateDatabase(historyDate, gids, uids))
	if err != nil {
		return err
	}

	return db.Close()
}

func (b *BaseDirs) updateDatabase(historyDate time.Time, gids, uids []uint32) func(*bolt.Tx) error {
	return func(tx *bolt.Tx) error {
		if err := clearUsageBuckets(tx); err != nil {
			return err
		}

		if err := createBucketsIfNotExist(tx); err != nil {
			return err
		}

		gidBase, err := b.gidsToBaseDirs(gids)
		if err != nil {
			return err
		}

		if errc := b.updateUsage(tx, gidBase, uids); errc != nil {
			return errc
		}

		return b.updateHistories(tx, historyDate, gidBase)
	}
}

func clearUsageBuckets(tx *bolt.Tx) error {
	if err := tx.DeleteBucket([]byte(groupUsageBucket)); err != nil && !errors.Is(err, bolt.ErrBucketNotFound) {
		return err
	}

	if err := tx.DeleteBucket([]byte(userUsageBucket)); err != nil && !errors.Is(err, bolt.ErrBucketNotFound) {
		return err
	}

	return nil
}

func createBucketsIfNotExist(tx *bolt.Tx) error {
	for _, bucket := range [...]string{groupUsageBucket, groupHistoricalBucket,
		groupSubDirsBucket, userUsageBucket, userSubDirsBucket} {
		if _, errc := tx.CreateBucketIfNotExists([]byte(bucket)); errc != nil {
			return errc
		}
	}

	return nil
}

func (b *BaseDirs) gidsToBaseDirs(gids []uint32) (map[uint32]dgut.DCSs, error) {
	gidBase := make(map[uint32]dgut.DCSs, len(gids))

	for _, gid := range gids {
		dcss, err := b.CalculateForGroup(gid)
		if err != nil {
			return nil, err
		}

		gidBase[gid] = dcss
	}

	return gidBase, nil
}

func (b *BaseDirs) updateUsage(tx *bolt.Tx, gidBase map[uint32]dgut.DCSs, uids []uint32) error {
	if errc := b.storeGIDBaseDirs(tx, gidBase); errc != nil {
		return errc
	}

	return b.storeUIDBaseDirs(tx, uids)
}

func (b *BaseDirs) storeGIDBaseDirs(tx *bolt.Tx, gidBase map[uint32]dgut.DCSs) error {
	gub := tx.Bucket([]byte(groupUsageBucket))

	for gid, dcss := range gidBase {
		for _, dcs := range dcss {
			keyName := strconv.FormatUint(uint64(gid), 10) + bucketKeySeparator + dcs.Dir
			quotaSize, quotaInode := b.quotas.Get(gid, dcs.Dir)
			uwm := &Usage{
				GID:         gid,
				BaseDir:     dcs.Dir,
				UsageSize:   dcs.Size,
				QuotaSize:   quotaSize,
				UsageInodes: dcs.Count,
				QuotaInodes: quotaInode,
				Mtime:       dcs.Mtime,
			}

			if err := gub.Put([]byte(keyName), b.encodeToBytes(uwm)); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *BaseDirs) encodeToBytes(data any) []byte {
	var encoded []byte
	enc := codec.NewEncoderBytes(&encoded, b.ch)
	enc.MustEncode(data)

	return encoded
}

func (b *BaseDirs) storeUIDBaseDirs(tx *bolt.Tx, uids []uint32) error {
	uub := tx.Bucket([]byte(userUsageBucket))

	for _, uid := range uids {
		dcss, err := b.CalculateForUser(uid)
		if err != nil {
			return err
		}

		for _, dcs := range dcss {
			keyName := strconv.FormatUint(uint64(uid), 10) + bucketKeySeparator + dcs.Dir
			uwm := &Usage{
				UID:         uid,
				BaseDir:     dcs.Dir,
				UsageSize:   dcs.Size,
				UsageInodes: dcs.Count,
				Mtime:       dcs.Mtime,
			}

			if err := uub.Put([]byte(keyName), b.encodeToBytes(uwm)); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *BaseDirs) updateHistories(tx *bolt.Tx, historyDate time.Time,
	gidBase map[uint32]dgut.DCSs) error {
	ghb := tx.Bucket([]byte(groupHistoricalBucket))

	gidMounts, err := b.gidsToMountpoints(gidBase)
	if err != nil {
		return err
	}

	for gid, mounts := range gidMounts {
		if err = b.updateGroupHistories(ghb, gid, mounts, historyDate); err != nil {
			return err
		}
	}

	return nil
}

type gidMountsMap map[uint32]map[string]dgut.DirSummary

func (b *BaseDirs) gidsToMountpoints(gidBase map[uint32]dgut.DCSs) (gidMountsMap, error) {
	gidMounts := make(gidMountsMap, len(gidBase))

	mps, err := getMountPoints()
	if err != nil {
		return nil, err
	}

	for gid, dcss := range gidBase {
		mounts := make(map[string]dgut.DirSummary)

		for _, dcs := range dcss {
			mp := mps.prefixOf(dcs.Dir)
			if mp != "" {
				ds := mounts[mp]

				ds.Count += dcs.Count
				ds.Size += dcs.Size

				mounts[mp] = ds
			}
		}

		gidMounts[gid] = mounts
	}

	return gidMounts, nil
}

func (b *BaseDirs) updateGroupHistories(ghb *bolt.Bucket, gid uint32,
	mounts map[string]dgut.DirSummary, historyDate time.Time) error {
	for mount, ds := range mounts {
		quotaSize, quotaInode := b.quotas.Get(gid, mount)

		key := []byte(strconv.FormatUint(uint64(gid), 10) + bucketKeySeparator + mount)

		existing := ghb.Get(key)

		histories, err := b.updateHistory(ds, quotaSize, quotaInode, historyDate, existing)
		if err != nil {
			return err
		}

		if err = ghb.Put(key, histories); err != nil {
			return err
		}
	}

	return nil
}

func (b *BaseDirs) updateHistory(ds dgut.DirSummary, quotaSize, quotaInode uint64,
	historyDate time.Time, existing []byte) ([]byte, error) {
	var histories []History

	if existing != nil {
		if err := b.decodeFromBytes(existing, &histories); err != nil {
			return nil, err
		}
	}

	histories = append(histories, History{
		Date:        historyDate,
		UsageSize:   ds.Size,
		UsageInodes: ds.Count,
		QuotaSize:   quotaSize,
		QuotaInodes: quotaInode,
	})

	return b.encodeToBytes(histories), nil
}

func (b *BaseDirs) decodeFromBytes(encoded []byte, data any) error {
	return codec.NewDecoderBytes(encoded, b.ch).Decode(data)
}

// GroupUsage returns the usage for every GID-BaseDir combination in the
// database.
func (b *BaseDirReader) GroupUsage() ([]*Usage, error) {
	return b.usage(groupUsageBucket)
}

func (b *BaseDirReader) usage(bucket string) ([]*Usage, error) {
	var uwms []*Usage

	if err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucket))

		return bucket.ForEach(func(_, data []byte) error {
			uwm := new(Usage)

			if err := b.decodeFromBytes(data, uwm); err != nil {
				return err
			}

			uwms = append(uwms, uwm)

			return nil
		})
	}); err != nil {
		return nil, err
	}

	return uwms, nil
}

func (b *BaseDirReader) decodeFromBytes(encoded []byte, data any) error {
	return codec.NewDecoderBytes(encoded, b.ch).Decode(data)
}

// UserUsage returns the usage for every UID-BaseDir combination in the
// database.
func (b *BaseDirReader) UserUsage() ([]*Usage, error) {
	return b.usage(userUsageBucket)
}
