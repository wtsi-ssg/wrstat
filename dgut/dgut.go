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

import "github.com/ugorji/go/codec"

// DGUT handles all the *GUT information for a directory.
type DGUT struct {
	Dir  string
	GUTs GUTs
}

// encodeToBytes returns our Dir as a []byte and our GUTs encoded in another
// []byte suitable for storing on disk.
func (d *DGUT) encodeToBytes(ch codec.Handle) ([]byte, []byte) {
	var encoded []byte
	enc := codec.NewEncoderBytes(&encoded, ch)
	enc.MustEncode(d.GUTs)

	return []byte(d.Dir), encoded
}

// decodeDGUTbytes converts the byte slices returned by DGUT.Encode() back in to
// a *DGUT.
func decodeDGUTbytes(ch codec.Handle, dir, encoded []byte) *DGUT {
	dec := codec.NewDecoderBytes(encoded, ch)

	var g GUTs

	dec.MustDecode(&g)

	return &DGUT{
		Dir:  string(dir),
		GUTs: g,
	}
}

// CountAndSize sums the count and size of all our GUTs and returns the results.
//
// See GUTs.CountAndSize for an explanation of the filter.
func (d *DGUT) CountAndSize(filter *Filter) (uint64, uint64) {
	return d.GUTs.CountAndSize(filter)
}

// Append appends the GUTs in the given DGUT to our own. Useful when you have
// 2 DGUTs for the same Dir that were calculated on different subdirectories
// independently, and now you're dealing with DGUTs for their common parent
// directories.
func (d *DGUT) Append(other *DGUT) {
	d.GUTs = append(d.GUTs, other.GUTs...)
}
