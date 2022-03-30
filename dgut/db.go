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
	"syscall"

	"github.com/hashicorp/go-multierror"
	"github.com/ugorji/go/codec"
	bolt "go.etcd.io/bbolt"
)

const (
	gutBucket          = "gut"
	childBucket        = "children"
	dbBasenameDGUT     = "dgut.db"
	dbBasenameChildren = dbBasenameDGUT + ".children"
	dbOpenMode         = 0600
)

const ErrDBExists = Error("database already exists")
const ErrDBNotExists = Error("database doesn't exists")
const ErrDirNotFound = Error("directory not found")

// a dbSet is 2 databases, one for storing DGUTs, one for storing children.
type dbSet struct {
	dir      string
	dguts    *bolt.DB
	children *bolt.DB
}

// newDBSet creates a new newDBSet that knows where its database files are
// located or should be created.
func newDBSet(dir string) *dbSet {
	return &dbSet{
		dir: dir,
	}
}

// Create creates new database files in our directory. Returns an error if those
// files already exist.
func (s *dbSet) Create() error {
	paths := s.paths()

	if s.pathsExist(paths) {
		return ErrDBExists
	}

	db, err := openBoltWritable(paths[0], gutBucket)
	if err != nil {
		return err
	}

	s.dguts = db

	db, err = openBoltWritable(paths[1], childBucket)
	s.children = db

	return err
}

// paths returns the expected paths for our dgut and children databases
// respectively.
func (s *dbSet) paths() []string {
	return []string{
		filepath.Join(s.dir, dbBasenameDGUT),
		filepath.Join(s.dir, dbBasenameChildren),
	}
}

// pathsExist tells you if the databases at the given paths already exist.
func (s *dbSet) pathsExist(paths []string) bool {
	for _, path := range paths {
		info, err := os.Stat(path)
		if err == nil && info.Size() != 0 {
			return true
		}
	}

	return false
}

// openBoltWritable creates a new database at the given path with the given
// bucket inside.
func openBoltWritable(path, bucket string) (*bolt.DB, error) {
	db, err := bolt.Open(path, dbOpenMode, &bolt.Options{
		NoFreelistSync: true,
		NoGrowSync:     true,
		FreelistType:   bolt.FreelistMapType,
	})
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, errc := tx.CreateBucketIfNotExists([]byte(bucket))

		return errc
	})

	return db, err
}

// Open opens our constituent databases read-only.
func (s *dbSet) Open() error {
	paths := s.paths()

	db, err := openBoltReadOnly(paths[0])
	if err != nil {
		return err
	}

	s.dguts = db

	db, err = openBoltReadOnly(paths[1])
	if err != nil {
		return err
	}

	s.children = db

	return nil
}

// openBoltReadOnly opens a bolt database at the given path in read-only mode.
func openBoltReadOnly(path string) (*bolt.DB, error) {
	return bolt.Open(path, dbOpenMode, &bolt.Options{
		ReadOnly:  true,
		MmapFlags: syscall.MAP_POPULATE,
	})
}

// Close closes our constituent databases.
func (s *dbSet) Close() error {
	var errm *multierror.Error

	err := s.dguts.Close()
	errm = multierror.Append(errm, err)

	err = s.children.Close()
	errm = multierror.Append(errm, err)

	return errm.ErrorOrNil()
}

// DB is used to create and query a database made from a dgut file, which is the
// directory,group,user,type summary output produced by the summary packages'
// DirGroupUserType.Output() method.
type DB struct {
	paths      []string
	writeSet   *dbSet
	readSets   []*dbSet
	batchSize  int
	writeBatch []*DGUT
	writeI     int
	writeErr   error
	ch         codec.Handle
}

// NewDB returns a *DB that can be used to create or query a dgut database.
// Provide the path to directory that (will) store(s) the database files. In the
// case of only reading databases with Open(), you can supply multiple directory
// paths to query all of them simultaneously.
func NewDB(paths ...string) *DB {
	return &DB{paths: paths}
}

// Store will read the given dgut file data (as output by
// summary.DirGroupUserType.Output()) and store it in 2 database files that
// offer fast lookup of the information by directory.
//
// The path for the database directory you provided to NewDB() (only the first
// will be used) must not already have database files in it to create a new
// database. You can't add to an existing database. If you create multiple sets
// of data to store, instead Store them to individual database directories, and
// then load all them together during Open().
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
		errc := d.writeSet.Close()
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

// createDB creates a new database set, but only if it doesn't already exist.
func (d *DB) createDB() error {
	set := newDBSet(d.paths[0])

	err := set.Create()
	if err != nil {
		return err
	}

	d.writeSet = set
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

	var errm *multierror.Error

	err := d.storeChildren()
	errm = multierror.Append(errm, err)

	err = d.storeDGUTs()
	errm = multierror.Append(errm, err)

	err = errm.ErrorOrNil()
	if err != nil {
		d.writeErr = err
	}
}

// storeChildren stores the Dirs of the current DGUT batch in the db.
func (d *DB) storeChildren() error {
	return d.writeSet.children.Update(d.storeChildrenInTx)
}

// storeChildrenInTx stores the current writeBatch directories in the given
// transaction.
func (d *DB) storeChildrenInTx(tx *bolt.Tx) error {
	b := tx.Bucket([]byte(childBucket))

	for _, dgut := range d.writeBatch {
		if dgut == nil {
			return nil
		}

		if err := d.storeChildInDB(b, dgut.Dir); err != nil {
			return err
		}
	}

	return nil
}

// storeChildInDB stores the given child directory in the database against its
// parent directory, adding to any existing children. Duplicate children should
// not be added.
//
// The root directory / is ignored since it is not a child and has no parent.
func (d *DB) storeChildInDB(b *bolt.Bucket, child string) error {
	if child == "/" {
		return nil
	}

	parent := filepath.Dir(child)

	children := d.getChildrenFromDB(b, parent)

	children = append(children, child)

	return b.Put([]byte(parent), d.encodeChildren(children))
}

// getChildrenFromDB retrieves the child directory values associated with the
// given directory key in the given db. Returns an empty slice if the dir wasn't
// found.
func (d *DB) getChildrenFromDB(b *bolt.Bucket, dir string) []string {
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

// storeDGUTs stores the current batch of DGUTs in the db.
func (d *DB) storeDGUTs() error {
	return d.writeSet.dguts.Update(d.storeDGUTsInTx)
}

// storeDGUTsInTx stores the current writeBatch DGUTs in the given transaction.
func (d *DB) storeDGUTsInTx(tx *bolt.Tx) error {
	b := tx.Bucket([]byte(gutBucket))

	for _, dgut := range d.writeBatch {
		if dgut == nil {
			return nil
		}

		if err := d.storeDGUT(b, dgut); err != nil {
			return err
		}
	}

	return nil
}

// storeDGUT stores a DGUT in the db. DGUTs are expected to be unique per
// Store() operation and database.
func (d *DB) storeDGUT(b *bolt.Bucket, dgut *DGUT) error {
	dir, guts := dgut.encodeToBytes(d.ch)

	return b.Put(dir, guts)
}

// Open opens the database(s) for reading. You need to call this before using
// the query methods like DirInfo() and Which(). You should call Close() after
// you've finished.
func (d *DB) Open() error {
	readSets := make([]*dbSet, len(d.paths))

	for i, path := range d.paths {
		readSet := newDBSet(path)

		if !readSet.pathsExist(readSet.paths()) {
			return ErrDBNotExists
		}

		err := readSet.Open()
		if err != nil {
			return err
		}

		readSets[i] = readSet
	}

	d.readSets = readSets

	d.ch = new(codec.BincHandle)

	return nil
}

// Close closes the database(s) after reading. You should call this once
// you've finished reading, but it's not necessary; errors are ignored.
func (d *DB) Close() {
	if d.readSets == nil {
		return
	}

	for _, readSet := range d.readSets {
		readSet.Close()
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

	for _, readSet := range d.readSets {
		if err := readSet.dguts.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(gutBucket))

			return getDGUTFromDBAndAppend(b, dir, d.ch, dgut)
		}); err != nil {
			notFound++
		}
	}

	if notFound == len(d.readSets) {
		return 0, 0, ErrDirNotFound
	}

	c, s := dgut.CountAndSize(filter)

	return c, s, nil
}

// getDGUTFromDBAndAppend calls getDGUTFromDB() and appends the result
// to the given dgut. If the given dgut is empty, it will be populated with the
// content of the result instead.
func getDGUTFromDBAndAppend(b *bolt.Bucket, dir string, ch codec.Handle, dgut *DGUT) error {
	thisDGUT, err := getDGUTFromDB(b, dir, ch)
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

// getDGUTFromDB gets and decodes a dgut from the given database.
func getDGUTFromDB(b *bolt.Bucket, dir string, ch codec.Handle) (*DGUT, error) {
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

	for _, readSet := range d.readSets {
		// no error is possible here, but the View function requires we return
		// one.
		//nolint:errcheck
		readSet.children.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(childBucket))

			for _, child := range d.getChildrenFromDB(b, dir) {
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
