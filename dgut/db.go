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

// package dgut lets you create and query a database made from dgut files.

package dgut

import (
	"io"

	bolt "go.etcd.io/bbolt"
)

const (
	gutBucket = "gut"
)

// DB is used to create and query a database made from a dgut file, which is the
// directory,group,user,type summary output produced by the summary packages'
// DirGroupUserType.Output() method.
type DB struct {
	path       string
	db         *bolt.DB
	batchSize  int
	writeBatch []*DGUT
	writeI     int
	writeErr   error
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
		errc := d.db.Close()
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
		_, errc := tx.CreateBucketIfNotExists([]byte(gutBucket))

		return errc
	})

	d.db = db

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

// storeBatch writes the current batch of DGUTs to the database.
func (d *DB) storeBatch() {
	if d.writeErr != nil {
		return
	}

	err := d.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(gutBucket))

		return storeDGUTsInBucket(d.writeBatch, b)
	})

	if err != nil {
		d.writeErr = err
	}
}

// storeDGUTsInBucket stores the current batch of DGUTs in the given bucket.
// Only call from within a database transaction.
func storeDGUTsInBucket(dguts []*DGUT, b *bolt.Bucket) error {
	for _, dgut := range dguts {
		if err := storeDGUTinDB(dgut, b); err != nil {
			return err
		}
	}

	return nil
}

// storeDGUT stores a DGUT in the given database bucket. Only call from within a
// database transaction.
func storeDGUTinDB(dgut *DGUT, b *bolt.Bucket) error {
	if dgut == nil {
		return nil
	}

	dir, guts := dgut.encodeToBytes()

	return b.Put(dir, guts)
}
