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

package summary

import (
	"fmt"
	"io/fs"
	"os"
	"sort"
	"strings"
	"syscall"
)

// DirGUTFileType is one of the special file types that the
// directory,group,user,filetype summaries group on.
type DirGUTFileType uint8

const (
	DGUTFileTypeCram DirGUTFileType = iota
	DGUTFileTypeBam
	DGUTFileTypeIndex
	DGUTFileTypeCompressed
	DGUTFileTypeUncompressed
	DGUTFileTypeCheckpoint
	DGUTFileTypeOther
	DGUTFileTypeTemp
)

const ErrInvalidType = Error("not a valid file type")

// String lets you convert a DirGUTFileType to a meaningful string.
func (d DirGUTFileType) String() string {
	return [...]string{"cram", "bam", "index", "compressed", "uncompressed", "checkpoint", "other", "temporary"}[d]
}

// FileTypeStringToDirGUTFileType converts the String() representation of a
// DirGUTFileType back in to a DirGUTFileType. Errors if an invalid string
// supplied.
func FileTypeStringToDirGUTFileType(ft string) (DirGUTFileType, error) {
	convert := map[string]DirGUTFileType{
		"cram":         DGUTFileTypeCram,
		"bam":          DGUTFileTypeBam,
		"index":        DGUTFileTypeIndex,
		"compressed":   DGUTFileTypeCompressed,
		"uncompressed": DGUTFileTypeUncompressed,
		"checkpoint":   DGUTFileTypeCheckpoint,
		"other":        DGUTFileTypeOther,
		"temporary":    DGUTFileTypeTemp,
	}

	dgft, ok := convert[ft]

	if !ok {
		return DGUTFileTypeOther, ErrInvalidType
	}

	return dgft, nil
}

// gutStore is a sortable map with gid,uid,filetype as keys and summaries as
// values.
type gutStore map[string]*summary

// add will auto-vivify a summary for the given key (which should have been
// generated with statToGUTKey()) and call add(size) on it.
func (store gutStore) add(key string, size int64) {
	s, ok := store[key]
	if !ok {
		s = &summary{}
		store[key] = s
	}

	s.add(size)
}

// sort returns a slice of our summary values, sorted by our dgut keys which are
// also returned.
func (store gutStore) sort() ([]string, []*summary) {
	return sortSummaryStore(store)
}

// dirToGUTStore is a sortable map of directory to gutStore.
type dirToGUTStore map[string]gutStore

// getGUTStore auto-vivifies a gutStore for the given dir and returns it.
func (store dirToGUTStore) getGUTStore(dir string) gutStore {
	gStore, ok := store[dir]
	if !ok {
		gStore = make(gutStore)
		store[dir] = gStore
	}

	return gStore
}

// sort returns a slice of our gutStore values, sorted by our directory keys
// which are also returned.
func (store dirToGUTStore) sort() ([]string, []gutStore) {
	keys := make([]string, len(store))
	i := 0

	for k := range store {
		keys[i] = k
		i++
	}

	sort.Strings(keys)

	s := make([]gutStore, len(keys))

	for i, k := range keys {
		s[i] = store[k]
	}

	return keys, s
}

// typeCheckers take a path and return true if the path is of their file type.
type typeChecker func(path string) bool

// DirGroupUserType is used to summarise file stats by directory, group, user
// and file type.
type DirGroupUserType struct {
	store        dirToGUTStore
	typeCheckers map[DirGUTFileType]typeChecker
}

// NewByDirGroupUserType returns a DirGroupUserType.
func NewByDirGroupUserType() *DirGroupUserType {
	return &DirGroupUserType{
		store: make(dirToGUTStore),
		typeCheckers: map[DirGUTFileType]typeChecker{
			DGUTFileTypeCram:         isCram,
			DGUTFileTypeBam:          isBam,
			DGUTFileTypeIndex:        isIndex,
			DGUTFileTypeCompressed:   isCompressed,
			DGUTFileTypeUncompressed: isUncompressed,
			DGUTFileTypeCheckpoint:   isCheckpoint,
		},
	}
}

// isCram tells you if path is named like a cram file.
func isCram(path string) bool {
	return hasSuffix(path, []string{".cram"})
}

// hasSuffix tells you if path has one of the given suffixes.
func hasSuffix(path string, suffixes []string) bool {
	lc := strings.ToLower(path)

	for _, suffix := range suffixes {
		if strings.HasSuffix(lc, suffix) {
			return true
		}
	}

	return false
}

// isBam tells you if path is named like a bam file.
func isBam(path string) bool {
	return hasSuffix(path, []string{".bam"})
}

// isIndex tells you if path is named like an index file.
func isIndex(path string) bool {
	return hasSuffix(path, []string{".crai", ".bai", ".sai", ".fai", ".csi"})
}

// isCompressed tells you if path is named like a compressed file.
func isCompressed(path string) bool {
	return hasSuffix(path, []string{".bzip2", ".gz", ".tgz", ".zip", ".xz",
		".bgz", ".bcf"})
}

// isUncompressed tells you if path is named like some standard known
// uncompressed files.
func isUncompressed(path string) bool {
	return hasSuffix(path, []string{".sam", ".fasta", ".fastq", ".fa", ".fq",
		".vcf", ".csv", ".tsv", ".txt", ".text", "readme", ".o", ".e", ".oe",
		".dat"})
}

// isCheckpoint tells you if path is named like a checkpoint file.
func isCheckpoint(path string) bool {
	return hasSuffix(path, []string{"jobstate.context"})
}

// isTemp tells you if path is named like a temporary file.
func isTemp(path string) bool {
	lc := strings.ToLower(path)

	for _, containing := range []string{"/tmp/", "/temp/", ".tmp", ".temp"} {
		if strings.Contains(lc, containing) {
			return true
		}
	}

	return false
}

// Add is a github.com/wtsi-ssg/wrstat/stat Operation. It will break path in to
// its directories and add the file size and increment the file count to each,
// summed for the info's group, user and filetype. If path is a directory, it is
// ignored.
//
// NB: the "temporary" filetype is an extra filetype on top of the other normal
// filetypes, so if you sum all the filetypes to get information about a a given
// directory+group+user combination, you should ignore "temporary". Only count
// "temporary" when it's the only type you're considering, or you'll count some
// files twice.
func (d *DirGroupUserType) Add(path string, info fs.FileInfo) error {
	if info.IsDir() {
		return nil
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errNotUnix
	}

	d.addForEachDir(path, d.statToGUTKeys(stat, path), info.Size())

	return nil
}

// statToGUTKey extracts gid and uid from the stat, determines the filetype
// from the path, and combines them into a group+user+type key. More than 1 key
// can be returned, because there is a "temporary" filetype as well as more
// specific types, and path could be both.
func (d *DirGroupUserType) statToGUTKeys(stat *syscall.Stat_t, path string) []string {
	types := d.pathToTypes(path)

	keys := make([]string, len(types))
	for i, t := range types {
		keys[i] = fmt.Sprintf("%d\t%d\t%d", stat.Gid, stat.Uid, t)
	}

	return keys
}

// pathToTypes determines the filetype of the given path based on its basename,
// and returns a slice of our DirGUTFileType. More than one is possible, because
// a path can be both a temporary file, and another type.
func (d *DirGroupUserType) pathToTypes(path string) []DirGUTFileType {
	var types []DirGUTFileType

	for ftype, isThisType := range d.typeCheckers {
		if isThisType(path) {
			types = append(types, ftype)
		}
	}

	if len(types) == 0 {
		types = append(types, DGUTFileTypeOther)
	}

	if isTemp(path) {
		types = append(types, DGUTFileTypeTemp)
	}

	return types
}

// addForEachDir breaks path into each directory, gets a gutStore for each and
// adds a file of the given size to them under the given gutKeys.
func (d *DirGroupUserType) addForEachDir(path string, gutKeys []string, size int64) {
	cb := func(dir string) {
		gStore := d.store.getGUTStore(dir)

		for _, gutKey := range gutKeys {
			gStore.add(gutKey, size)
		}
	}

	doForEachDir(path, cb)
}

// Output will write summary information for all the paths previously added. The
// format is (tab separated):
//
// directory gid uid filetype filecount filesize
//
// directory, gid, uid and filetype are sorted. The sort on the columns is not
// numeric, but alphabetical. So gid 10 will come before gid 2.
//
// filetype is one of our filetype ints:
//
//   0 = cram
//   1 = bam
//   2 = index
//   3 = compressed
//   4 = uncompressed
//   5 = checkpoint
//   6 = other
//   7 = temp
//
// Returns an error on failure to write. output is closed on completion.
func (d *DirGroupUserType) Output(output *os.File) error {
	dirs, gStores := d.store.sort()

	for i, dir := range dirs {
		dguts, summaries := gStores[i].sort()

		for j, dgut := range dguts {
			s := summaries[j]
			_, errw := output.WriteString(fmt.Sprintf("%s\t%s\t%d\t%d\n", dir, dgut, s.count, s.size))

			if errw != nil {
				return errw
			}
		}
	}

	return output.Close()
}
