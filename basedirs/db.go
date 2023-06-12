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
	"strconv"
	"time"

	"github.com/ugorji/go/codec"
	bolt "go.etcd.io/bbolt"
)

const dbOpenMode = 0600
const bucketKeySeparator = "-"

const (
	groupUsageBucket      = "groupUsage"
	groupHistoricalBucket = "groupHistoricalQuota"
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
func (b *BaseDirs) CreateDatabase() error {
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

	err = db.Update(func(tx *bolt.Tx) error {
		if errc := createBucketsIfNotExit(tx); errc != nil {
			return errc
		}

		if errc := b.storeGIDBaseDirs(tx, gids); errc != nil {
			return errc
		}

		return b.storeUIDBaseDirs(tx, uids)
	})
	if err != nil {
		return err
	}

	return db.Close()
}

func createBucketsIfNotExit(tx *bolt.Tx) error {
	for _, bucket := range [...]string{groupUsageBucket, groupHistoricalBucket,
		groupSubDirsBucket, userUsageBucket, userSubDirsBucket} {
		if _, errc := tx.CreateBucketIfNotExists([]byte(bucket)); errc != nil {
			return errc
		}
	}

	return nil
}

func (b *BaseDirs) storeGIDBaseDirs(tx *bolt.Tx, gids []uint32) error {
	gub := tx.Bucket([]byte(groupUsageBucket))

	for _, gid := range gids {
		dcss, err := b.CalculateForGroup(gid)
		if err != nil {
			return err
		}

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

// GroupUsage returns the usage for every GID-BaseDir combination in the
// database.
func (b *BaseDirReader) GroupUsage() ([]*Usage, error) {
	return b.usage(groupUsageBucket)
}

func (b *BaseDirReader) usage(bucket string) ([]*Usage, error) {
	var uwms []*Usage

	if err := b.db.View(func(tx *bolt.Tx) error {
		gub := tx.Bucket([]byte(bucket))

		return gub.ForEach(func(_, data []byte) error {
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
