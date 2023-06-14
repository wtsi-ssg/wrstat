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
	"github.com/ugorji/go/codec"
	bolt "go.etcd.io/bbolt"
)

// BaseDirReader is used to read the information stored in a BaseDir database.
type BaseDirReader struct {
	db          *bolt.DB
	ch          codec.Handle
	mountPoints mountPoints
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

func (b *BaseDirReader) WeaverBasedirOutput() (string, error) {
	return "", nil
}

func (b *BaseDirReader) WeaverSubdirOutput(gid uint32, basedir string) (string, error) {
	return "", nil
}
