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

import "github.com/wtsi-ssg/wrstat/summary"

// GUT handles group,user,type,count,size information.
type GUT struct {
	GID   uint32
	UID   uint32
	FT    summary.DirGUTFileType
	Count uint64
	Size  uint64
}

// Filter can be applied to a GUT to see if it has one of the specified GIDs,
// UIDs and FTs, in which case it passes the filter.
//
// If the Filter has one of those properties set to nil, or the whole Filter is
// nil, a GUT will be considered to pass the filter.
//
// The exeception to this is when FTs != []{DGUTFileTypeTemp}, and the GUT has
// an FT of DGUTFileTypeTemp. A GUT for a temporary file will always fail to
// pass the filter unless filtering specifically for temporary files, because
// other GUT objects will represent the same file on disk but with another file
// type, and you won't want to double-count.
type Filter struct {
	GIDs []uint32
	UIDs []uint32
	FTs  []summary.DirGUTFileType
}

// PassesFilter checks to see if this GUT has a GID in the filter's GIDs
// (considered true if GIDs is nil), and has a UID in the filter's UIDs
// (considered true if UIDs is nil), and has an FT in the filter's FTs
// (considered true if FTs is nil, unless FT is DGUTFileTypeTemp, in which case
// will return false, unless FTs == []{DGUTFileTypeTemp}).
func (g *GUT) PassesFilter(filter *Filter) bool {
	if !g.passesGIDFilter(filter) {
		return false
	}

	if !g.passesUIDFilter(filter) {
		return false
	}

	return g.passesFTFilter(filter)
}

// passesGIDFilter tells you if our GID is in the filter's GIDs. Also returns
// true if filter or filter.GIDs in nil.
func (g *GUT) passesGIDFilter(filter *Filter) bool {
	if filter == nil || filter.GIDs == nil {
		return true
	}

	for _, gid := range filter.GIDs {
		if gid == g.GID {
			return true
		}
	}

	return false
}

// passesUIDFilter tells you if our UID is in the filter's UIDs. Also returns
// true if filter or filter.UIDs in nil.
func (g *GUT) passesUIDFilter(filter *Filter) bool {
	if filter == nil || filter.UIDs == nil {
		return true
	}

	for _, uid := range filter.UIDs {
		if uid == g.UID {
			return true
		}
	}

	return false
}

// passesFTFilter tells you if our FT is in the filter's FTs. Also returns true
// if filter or filter.FTs in nil.
//
// However, if our FT is DGUTFileTypeTemp, then always returns false, unless the
// filter's FTs only hold DGUTFileTypeTemp.
func (g *GUT) passesFTFilter(filter *Filter) bool {
	if filter == nil || filter.FTs == nil {
		return g.FT != summary.DGUTFileTypeTemp
	}

	for _, ft := range filter.FTs {
		if ft == g.FT {
			return !g.amTempAndNotFilteredJustForTemp(filter)
		}
	}

	return false
}

// amTempAndNotFilteredJustForTemp tells you if our FT is DGUTFileTypeTemp and
// the filter has more than one type set.
func (g *GUT) amTempAndNotFilteredJustForTemp(filter *Filter) bool {
	return g.FT == summary.DGUTFileTypeTemp && len(filter.FTs) > 1
}

// GUTs is a slice of *GUT, offering ways to filter and sum the information in
// our *GUTs.
type GUTs []*GUT

// CountAndSize sums the count and size of all our GUT elements and returns the
// results.
//
// Provide a Filter to ignore GUT elements that do not match one of the
// specified GIDs, one of the UIDs, and one of the FTs. If one of those
// properties is nil, does not filter on that property.
//
// Provide nil to do no filtering.
//
// Note that FT 7 is "temporary" files, and because a file can be both
// temporary and another type, if your Filter's FTs slice doesn't contain
// just DGUTFileTypeTemp, any GUT with FT DGUTFileTypeTemp is always ignored.
func (g GUTs) CountAndSize(filter *Filter) (uint64, uint64) {
	var c, s uint64

	for _, gut := range g {
		if !gut.PassesFilter(filter) {
			continue
		}

		c += gut.Count
		s += gut.Size
	}

	return c, s
}
