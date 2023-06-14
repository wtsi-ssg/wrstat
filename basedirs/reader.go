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
	"fmt"
	"strings"
	"time"

	"github.com/ugorji/go/codec"
	bolt "go.etcd.io/bbolt"
)

const secondsInDay = time.Hour * 24
const threeDays = 3 * secondsInDay
const quotaStatusOK = "OK"
const quotaStatusNotOK = "Not OK"

// BaseDirReader is used to read the information stored in a BaseDir database.
type BaseDirReader struct {
	db          *bolt.DB
	ch          codec.Handle
	mountPoints mountPoints
	groupCache  GroupCache
	userCache   UserCache
}

// NewReader returns a BaseDirReader that can return the summary information
// stored in a BaseDir database.
func NewReader(path string) (*BaseDirReader, error) {
	db, err := bolt.Open(path, dbOpenMode, &bolt.Options{
		ReadOnly: true,
	})
	if err != nil {
		return nil, err
	}

	mp, err := getMountPoints()
	if err != nil {
		return nil, err
	}

	return &BaseDirReader{
		db:          db,
		ch:          new(codec.BincHandle),
		mountPoints: mp,
		groupCache:  make(GroupCache),
		userCache:   make(UserCache),
	}, nil
}

func (b *BaseDirReader) Close() error {
	return b.db.Close()
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

// GroupSubDirs returns a slice of SubDir, one for each subdirectory of the
// given basedir, owned by the given group. If basedir directly contains files,
// one of the SubDirs will be for ".".
func (b *BaseDirReader) GroupSubDirs(gid uint32, basedir string) ([]*SubDir, error) {
	return b.subDirs(groupSubDirsBucket, gid, basedir)
}

func (b *BaseDirReader) subDirs(bucket string, id uint32, basedir string) ([]*SubDir, error) {
	var sds []*SubDir

	if err := b.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(bucket))
		data := bucket.Get(keyName(id, basedir))

		if data == nil {
			return nil
		}

		return b.decodeFromBytes(data, &sds)
	}); err != nil {
		return nil, err
	}

	return sds, nil
}

// UserSubDirs returns a slice of SubDir, one for each subdirectory of the
// given basedir, owned by the given user. If basedir directly contains files,
// one of the SubDirs will be for ".".
func (b *BaseDirReader) UserSubDirs(uid uint32, basedir string) ([]*SubDir, error) {
	return b.subDirs(userSubDirsBucket, uid, basedir)
}

// GroupUsageTable returns GroupUsage() information formatted with the following
// tab separated columns:
//
// used
// quota
// last_modified
// directory_path
// warning
// pi_name
// group_name
//
// Any error returned is from GroupUsage().
func (b *BaseDirReader) GroupUsageTable() (string, error) {
	gu, err := b.GroupUsage()
	if err != nil {
		return "", err
	}

	return usageTable(gu, b.History, func(u *Usage) string {
		return b.groupCache.GroupName(u.GID)
	})
}

func usageTable(usage []*Usage, historyCB func(gid uint32, path string) ([]History, error),
	nameCB func(*Usage) string) (string, error) {
	var sb strings.Builder

	for _, u := range usage {
		h, err := historyCB(u.GID, u.BaseDir)
		if err != nil {
			return "", err
		}

		fmt.Fprintf(&sb, "%d\t%d\t%d\t%s\t%s\t%s\t%s\n",
			u.UsageSize,
			u.QuotaSize,
			daysSince(u.Mtime),
			u.BaseDir,
			usageStatus(h),
			"",
			nameCB(u),
		)
	}

	return sb.String(), nil
}

func usageStatus(h []History) string {
	sizeExceedDate, inodeExceedDate := DateQuotaFull(h)
	threeDaysFromNow := time.Now().Add(threeDays)

	if !sizeExceedDate.IsZero() && threeDaysFromNow.After(sizeExceedDate) {
		return quotaStatusNotOK
	}

	if !inodeExceedDate.IsZero() && threeDaysFromNow.After(inodeExceedDate) {
		return quotaStatusNotOK
	}

	return quotaStatusOK
}

// UserUsageTable returns UserUsage() information formatted with the following
// tab separated columns:
//
// used
// quota (currently always zero)
// last_modified
// directory_path
// warning
// pi_name
// user_name
//
// Any error returned is from UserUsage().
func (b *BaseDirReader) UserUsageTable() (string, error) {
	uu, err := b.UserUsage()
	if err != nil {
		return "", err
	}

	return usageTable(uu, func(_ uint32, _ string) ([]History, error) {
		return nil, nil
	}, func(u *Usage) string {
		return b.userCache.UserName(u.UID)
	})
}

func daysSince(mtime time.Time) uint64 {
	return uint64(time.Since(mtime) / secondsInDay)
}

// GroupSubDirUsageTable returns GroupSubDirs() information formatted with the
// following tab separated columns:
//
// base_directory_path
// sub_directory
// num_files
// size
// last_modified
// filetypes
//
// Any error returned is from GroupSubDirs().
func (b *BaseDirReader) GroupSubDirUsageTable(gid uint32, basedir string) (string, error) {
	gsdut, err := b.GroupSubDirs(gid, basedir)
	if err != nil {
		return "", err
	}

	return subDirUsageTable(basedir, gsdut), nil
}

func subDirUsageTable(basedir string, subdirs []*SubDir) string {
	var sb strings.Builder

	for _, subdir := range subdirs {
		fmt.Fprintf(&sb, "%s\t%s\t%d\t%d\t%d\t%s\n",
			basedir,
			subdir.SubDir,
			subdir.NumFiles,
			subdir.SizeFiles,
			daysSince(subdir.LastModified),
			subdir.FileUsage,
		)
	}

	return sb.String()
}

// UserSubDirUsageTable returns UserSubDirs() information formatted with the
// following tab separated columns:
//
// base_directory_path
// sub_directory
// num_files
// size
// last_modified
// filetypes
//
// Any error returned is from UserSubDirUsageTable().
func (b *BaseDirReader) UserSubDirUsageTable(uid uint32, basedir string) (string, error) {
	usdut, err := b.UserSubDirs(uid, basedir)
	if err != nil {
		return "", err
	}

	return subDirUsageTable(basedir, usdut), nil
}
