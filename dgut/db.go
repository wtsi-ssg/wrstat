/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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

package dgut

import (
	"io"
	"path/filepath"

	"github.com/hashicorp/go-multierror"
	"github.com/ugorji/go/codec"
	bolt "go.etcd.io/bbolt"
)

const (
	gutBucket   = "gut"
	childBucket = "children"
)

const ErrDirNotFound = Error("directory not found")

// DB is used to create and query a database made from a dgut file, which is the
// directory,group,user,type summary output produced by the summary packages'
// DirGroupUserType.Output() method.
type DB struct {
	path       string
	wdb        *bolt.DB
	rdb        *bolt.DB
	batchSize  int
	writeBatch []*DGUT
	writeI     int
	writeErr   error
	ch         codec.Handle
}

// NewDB returns a *DB that can be used to create or query a dgut database.
// Provide the path to the database file.
func NewDB(path string) *DB {
	return &DB{path: path}
}

// Store will read the given dgut file data (as output by
// summary.DirGroupUserType.Output()) and store it in a database file that
// offers fast lookup of the information by directory.
//
// It is assumed and recommended that the path for the database file you
// provided to NewDB() is the path to a non-existent file. Ie. only Store() to
// a given database file once. However, updates are possible.
//
// batchSize is how many directories worth of information are written to the
// database in one go. More is faster, but uses more memory. 10,000 might be a
// good number to try.
func (d *DB) Store(data io.Reader, batchSize int) error {
	d.batchSize = batchSize

	err := d.createDB()
	if err != nil {
		return err
	}

	defer func() {
		errc := d.wdb.Close()
		if err == nil {
			err = errc
		}
	}()

	if err = d.storeData(data); err != nil {
		return err
	}

	if d.writeBatch[0] != nil {
		d.storeBatch()
	}

	err = d.writeErr

	return err
}

// createDB creates a new database file if it doesn't already exist, and creates
// buckets inside it if they don't exist.
func (d *DB) createDB() error {
	db, err := bolt.Open(d.path, 0600, nil)
	if err != nil {
		return err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		var errm *multierror.Error

		_, errc := tx.CreateBucketIfNotExists([]byte(gutBucket))
		errm = multierror.Append(errm, errc)

		_, errc = tx.CreateBucketIfNotExists([]byte(childBucket))
		errm = multierror.Append(errm, errc)

		return errm.ErrorOrNil()
	})

	d.wdb = db
	d.ch = new(codec.BincHandle)

	return err
}

// storeData parses the data and stores it in our database file. Only call this
// after calling createDB(), and only call it once.
func (d *DB) storeData(data io.Reader) error {
	d.resetBatch()

	return parseDGUTLines(data, d.parserCB)
}

// resetBatch prepares us to receive a new batch of DGUTs from the parser.
func (d *DB) resetBatch() {
	d.writeBatch = make([]*DGUT, d.batchSize)
	d.writeI = 0
}

// parserCB is a dgutParserCallBack that is called during parsing of dgut file
// data. It batches up the DGUTs we receive, and writes them to the database
// when a batch is full.
func (d *DB) parserCB(dgut *DGUT) {
	d.writeBatch[d.writeI] = dgut
	d.writeI++

	if d.writeI == d.batchSize {
		d.storeBatch()
		d.resetBatch()
	}
}

// storeBatch writes the current batch of DGUTs to the database. It also updates
// our dir->child lookup in the database.
func (d *DB) storeBatch() {
	if d.writeErr != nil {
		return
	}

	if err := d.wdb.Update(d.storeChildrenAndDGUTs); err != nil {
		d.writeErr = err
	}
}

// storeChildrenAndDGUTs calls storeChildrenInBucket() and storeDGUTsInBucket()
// with the right buckets. Provide a database transaction from which to work
// inside.
func (d *DB) storeChildrenAndDGUTs(tx *bolt.Tx) error {
	var errm *multierror.Error

	b := tx.Bucket([]byte(childBucket))
	err := d.storeChildrenInBucket(b)
	errm = multierror.Append(errm, err)

	b = tx.Bucket([]byte(gutBucket))
	err = d.storeDGUTsInBucket(b)
	errm = multierror.Append(errm, err)

	return errm.ErrorOrNil()
}

// storeChildrenInBucket stores the Dirs of the current DGUT batch in the given
// bucket (which should be the childBucket). Only call from within a database
// transaction.
func (d *DB) storeChildrenInBucket(b *bolt.Bucket) error {
	for _, dgut := range d.writeBatch {
		if dgut == nil {
			return nil
		}

		if err := d.storeChildInDB(dgut.Dir, b); err != nil {
			return err
		}
	}

	return nil
}

// storeChildInDB stores the given child directory in the given database bucket
// (which should be the childBucket) against its parent directory, adding to any
// existing children. Only call from within a database transaction.
//
// The root directory / is ignored since it is not a child and has no parent.
func (d *DB) storeChildInDB(child string, b *bolt.Bucket) error {
	if child == "/" {
		return nil
	}

	parent := filepath.Dir(child)

	children := d.getChildrenFromBucket(parent, b)
	children = append(children, child)

	return b.Put([]byte(parent), d.encodeChildren(children))
}

// getChildrenFromBucket retrieves the child directory values associated with
// the given directory key in the given bucket (which should be childBucket).
// Only call from within a database transaction. Returns an empty slice if the
// dir wasn't found.
func (d *DB) getChildrenFromBucket(dir string, b *bolt.Bucket) []string {
	v := b.Get([]byte(dir))

	if v == nil {
		return []string{}
	}

	return d.decodeChildrenBytes(v)
}

// decodeChildBytes converts the byte slice returned by encodeChildren() back
// in to a []string.
func (d *DB) decodeChildrenBytes(encoded []byte) []string {
	dec := codec.NewDecoderBytes(encoded, d.ch)

	var children []string

	dec.MustDecode(&children)

	return children
}

// encodeChildren returns converts the given string slice into a []byte suitable
// for storing on disk.
func (d *DB) encodeChildren(dirs []string) []byte {
	var encoded []byte
	enc := codec.NewEncoderBytes(&encoded, d.ch)
	enc.MustEncode(dirs)

	return encoded
}

// storeDGUTsInBucket stores the current batch of DGUTs in the given bucket
// (which should be the gutBucket). Only call from within a database
// transaction.
func (d *DB) storeDGUTsInBucket(b *bolt.Bucket) error {
	for _, dgut := range d.writeBatch {
		if dgut == nil {
			return nil
		}

		if err := d.storeDGUTinDB(dgut, b); err != nil {
			return err
		}
	}

	return nil
}

// storeDGUT stores a DGUT in the given database bucket (which should be the
// gutBucket). Only call from within a database transaction.
func (d *DB) storeDGUTinDB(dgut *DGUT, b *bolt.Bucket) error {
	dir, guts := dgut.encodeToBytes(d.ch)

	return b.Put(dir, guts)
}

// Open opens the database for reading. You need to call this before using the
// query methods like DirInfo().
func (d *DB) Open() error {
	rdb, err := openBoltReadOnly(d.path)
	if err != nil {
		return err
	}

	d.rdb = rdb

	d.ch = new(codec.BincHandle)

	return nil
}

// openBoltReadOnly opens a bolt database at the given path in read-only mode.
func openBoltReadOnly(path string) (*bolt.DB, error) {
	return bolt.Open(path, 0666, &bolt.Options{ReadOnly: true})
}

// DirInfo tells you the total number of files and their total size nested under
// the given directory. See GUTs.CountAndSize for an explanation of the filter.
//
// Returns an error if dir doesn't exist.
//
// You must call Open() before calling this.
func (d *DB) DirInfo(dir string, filter *Filter) (uint64, uint64, error) {
	var dgut *DGUT

	if err := d.rdb.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(gutBucket))
		bdir := []byte(dir)
		v := b.Get(bdir)

		if v == nil {
			return ErrDirNotFound
		}

		dgut = decodeDGUTbytes(d.ch, bdir, v)

		return nil
	}); err != nil {
		return 0, 0, err
	}

	c, s := dgut.CountAndSize(filter)

	return c, s, nil
}

// Children returns the directory paths that are directly inside the given
// directory.
//
// Returns an error if there was a problem reading from the database, but no
// error and an empty slice if dir had no children (because it was a leaf dir,
// or didn't exist at all).
//
// You must call Open() before calling this.
func (d *DB) Children(dir string) []string {
	var children []string

	// no error is possible here, but the View function requires we return one.
	//nolint:errcheck
	d.rdb.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(childBucket))

		children = d.getChildrenFromBucket(dir, b)

		return nil
	})

	return children
}
