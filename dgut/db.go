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
	"os"
	"path/filepath"
	"sort"

	"github.com/hashicorp/go-multierror"
	"github.com/ugorji/go/codec"
	bolt "go.etcd.io/bbolt"
)

const (
	gutBucket   = "gut"
	childBucket = "children"
	dbOpenMode  = 0600
)

const ErrDBExists = Error("database file already exists")
const ErrDirNotFound = Error("directory not found")

// DB is used to create and query a database made from a dgut file, which is the
// directory,group,user,type summary output produced by the summary packages'
// DirGroupUserType.Output() method.
type DB struct {
	paths      []string
	wdb        *bolt.DB
	rdbs       []*bolt.DB
	batchSize  int
	writeBatch []*DGUT
	writeI     int
	writeErr   error
	ch         codec.Handle
}

// NewDB returns a *DB that can be used to create or query a dgut database.
// Provide the path to the database file. In the case of only reading databases
// with Open(), you can supply multiple database paths to query all of them
// simultaneously.
func NewDB(paths ...string) *DB {
	return &DB{paths: paths}
}

// Store will read the given dgut file data (as output by
// summary.DirGroupUserType.Output()) and store it in a database file that
// offers fast lookup of the information by directory.
//
// The path for the database file you provided to NewDB() (only the first will
// be used) must be a non-existent file to create a new database. You can't add
// to an existing database. If you create multiple sets of data to store,
// instead Store them to individual database files, and then load all them
// together during Open().
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
	info, err := os.Stat(d.paths[0])
	if err == nil && info.Size() != 0 {
		return ErrDBExists
	}

	db, err := bolt.Open(d.paths[0], dbOpenMode, nil)
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
// existing children. Duplicate children should not be added. Only call from
// within a database transaction.
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
// gutBucket). Only call from within a database transaction. DGUTs are expected
// to be unique per Store() operation and database.
func (d *DB) storeDGUTinDB(dgut *DGUT, b *bolt.Bucket) error {
	dir, guts := dgut.encodeToBytes(d.ch)

	return b.Put(dir, guts)
}

// Open opens the database for reading. You need to call this before using the
// query methods like DirInfo(). Be sure to call Close() after you're finished!
func (d *DB) Open() error {
	rdbs := make([]*bolt.DB, len(d.paths))

	for i, path := range d.paths {
		rdb, err := openBoltReadOnly(path)
		if err != nil {
			return err
		}

		rdbs[i] = rdb
	}

	d.rdbs = rdbs

	d.ch = new(codec.BincHandle)

	return nil
}

// openBoltReadOnly opens a bolt database at the given path in read-only mode.
func openBoltReadOnly(path string) (*bolt.DB, error) {
	return bolt.Open(path, dbOpenMode, &bolt.Options{ReadOnly: true})
}

// Close closes the database after reading. It's nice to call this once you've
// finished reading, but not necessary; errors are ignored.
func (d *DB) Close() {
	if d.rdbs == nil {
		return
	}

	for _, rdb := range d.rdbs {
		rdb.Close()
	}
}

// DirInfo tells you the total number of files and their total size nested under
// the given directory. See GUTs.CountAndSize for an explanation of the filter.
//
// Returns an error if dir doesn't exist.
//
// You must call Open() before calling this.
func (d *DB) DirInfo(dir string, filter *Filter) (uint64, uint64, error) {
	var notFound int

	dgut := &DGUT{}

	for _, rdb := range d.rdbs {
		if err := rdb.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(gutBucket))

			return getDGUTFromBucketAndAppend(b, dir, d.ch, dgut)
		}); err != nil {
			notFound++
		}
	}

	if notFound == len(d.rdbs) {
		return 0, 0, ErrDirNotFound
	}

	c, s := dgut.CountAndSize(filter)

	return c, s, nil
}

// getDGUTFromBucketAndAppend calls getDGUTFromBucket() and appends the result
// to the given dgut. If the given dgut is empty, it will be populated with the
// content of the result instead.
func getDGUTFromBucketAndAppend(b *bolt.Bucket, dir string, ch codec.Handle, dgut *DGUT) error {
	thisDGUT, err := getDGUTFromBucket(b, dir, ch)
	if err != nil {
		return err
	}

	if dgut.Dir == "" {
		dgut.Dir = thisDGUT.Dir
		dgut.GUTs = thisDGUT.GUTs
	} else {
		dgut.Append(thisDGUT)
	}

	return nil
}

// getDGUTFromBucket gets and decodes a dgut from the given bucket (which should
// be dgutBucket). Only call from within a database transaction.
func getDGUTFromBucket(b *bolt.Bucket, dir string, ch codec.Handle) (*DGUT, error) {
	bdir := []byte(dir)
	v := b.Get(bdir)

	if v == nil {
		return nil, ErrDirNotFound
	}

	dgut := decodeDGUTbytes(ch, bdir, v)

	return dgut, nil
}

// Children returns the directory paths that are directly inside the given
// directory.
//
// Returns an error if there was a problem reading from the database, but no
// error and an empty slice if dir had no children (because it was a leaf dir,
// or didn't exist at all).
//
// The same children from multiple databases are de-duplicated.
//
// You must call Open() before calling this.
func (d *DB) Children(dir string) []string {
	children := make(map[string]bool)

	for _, rdb := range d.rdbs {
		// no error is possible here, but the View function requires we return
		// one.
		//nolint:errcheck
		rdb.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(childBucket))

			for _, child := range d.getChildrenFromBucket(dir, b) {
				children[child] = true
			}

			return nil
		})
	}

	return mapToSortedKeys(children)
}

// mapToSortedKeys takes the keys from the given map and returns them as a
// sorted slice. If map length is 0, returns nil.
func mapToSortedKeys(things map[string]bool) []string {
	if len(things) == 0 {
		return nil
	}

	keys := make([]string, len(things))
	i := 0

	for thing := range things {
		keys[i] = thing
		i++
	}

	sort.Strings(keys)

	return keys
}
