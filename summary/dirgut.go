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
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

// DirGUTFileType is one of the special file types that the
// directory,group,user,filetype summaries group on.
type DirGUTFileType uint8

const (
	DGUTFileTypeOther      DirGUTFileType = 0
	DGUTFileTypeTemp       DirGUTFileType = 1
	DGUTFileTypeVCF        DirGUTFileType = 2
	DGUTFileTypeVCFGz      DirGUTFileType = 3
	DGUTFileTypeBCF        DirGUTFileType = 4
	DGUTFileTypeSam        DirGUTFileType = 5
	DGUTFileTypeBam        DirGUTFileType = 6
	DGUTFileTypeCram       DirGUTFileType = 7
	DGUTFileTypeFasta      DirGUTFileType = 8
	DGUTFileTypeFastq      DirGUTFileType = 9
	DGUTFileTypeFastqGz    DirGUTFileType = 10
	DGUTFileTypePedBed     DirGUTFileType = 11
	DGUTFileTypeCompressed DirGUTFileType = 12
	DGUTFileTypeText       DirGUTFileType = 13
	DGUTFileTypeLog        DirGUTFileType = 14
)

const ErrInvalidType = Error("not a valid file type")

// String lets you convert a DirGUTFileType to a meaningful string.
func (d DirGUTFileType) String() string {
	return [...]string{"other", "temp", "vcf", "vcf.gz", "bcf", "sam", "bam",
		"cram", "fasta", "fastq", "fastq.gz", "ped/bed", "compressed", "text", "log"}[d]
}

// FileTypeStringToDirGUTFileType converts the String() representation of a
// DirGUTFileType back in to a DirGUTFileType. Errors if an invalid string
// supplied.
func FileTypeStringToDirGUTFileType(ft string) (DirGUTFileType, error) {
	convert := map[string]DirGUTFileType{
		"other":      DGUTFileTypeOther,
		"temp":       DGUTFileTypeTemp,
		"vcf":        DGUTFileTypeVCF,
		"vcf.gz":     DGUTFileTypeVCFGz,
		"bcf":        DGUTFileTypeBCF,
		"sam":        DGUTFileTypeSam,
		"bam":        DGUTFileTypeBam,
		"cram":       DGUTFileTypeCram,
		"fasta":      DGUTFileTypeFasta,
		"fastq":      DGUTFileTypeFastq,
		"fastq.gz":   DGUTFileTypeFastqGz,
		"ped/bed":    DGUTFileTypePedBed,
		"compressed": DGUTFileTypeCompressed,
		"text":       DGUTFileTypeText,
		"log":        DGUTFileTypeLog,
	}

	dgft, ok := convert[ft]

	if !ok {
		return DGUTFileTypeOther, ErrInvalidType
	}

	return dgft, nil
}

// gutStore is a sortable map with gid,uid,filetype as keys and summaryWithAtime
// as values.
type gutStore map[string]*summaryWithAtime

// add will auto-vivify a summary for the given key (which should have been
// generated with statToGUTKey()) and call add(size, atime, mtime) on it.
func (store gutStore) add(key string, size int64, atime int64, mtime int64) {
	s, ok := store[key]
	if !ok {
		s = &summaryWithAtime{}
		store[key] = s
	}

	s.add(size, atime, mtime)
}

// sort returns a slice of our summaryWithAtime values, sorted by our dgut keys
// which are also returned.
func (store gutStore) sort() ([]string, []*summaryWithAtime) {
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
			DGUTFileTypeTemp:       isTemp,
			DGUTFileTypeVCF:        isVCF,
			DGUTFileTypeVCFGz:      isVCFGz,
			DGUTFileTypeBCF:        isBCF,
			DGUTFileTypeSam:        isSam,
			DGUTFileTypeBam:        isBam,
			DGUTFileTypeCram:       isCram,
			DGUTFileTypeFasta:      isFasta,
			DGUTFileTypeFastq:      isFastq,
			DGUTFileTypeFastqGz:    isFastqGz,
			DGUTFileTypePedBed:     isPedBed,
			DGUTFileTypeCompressed: isCompressed,
			DGUTFileTypeText:       isText,
			DGUTFileTypeLog:        isLog,
		},
	}
}

// isTemp tells you if path is named like a temporary file.
func isTemp(path string) bool {
	if hasOneOfSuffixes(path, []string{".tmp", ".temp"}) {
		return true
	}

	lc := strings.ToLower(path)

	for _, containing := range []string{"/tmp/", "/temp/"} {
		if strings.Contains(lc, containing) {
			return true
		}
	}

	base := filepath.Base(lc)

	for _, prefix := range []string{".tmp.", "tmp.", ".temp.", "temp."} {
		if strings.HasPrefix(base, prefix) {
			return true
		}
	}

	return false
}

// hasOneOfSuffixes tells you if path has one of the given suffixes.
func hasOneOfSuffixes(path string, suffixes []string) bool {
	lc := strings.ToLower(path)

	for _, suffix := range suffixes {
		if strings.HasSuffix(lc, suffix) {
			return true
		}
	}

	return false
}

// isVCF tells you if path is named like a vcf file.
func isVCF(path string) bool {
	return hasSuffix(path, ".vcf")
}

// hasSuffix tells you if path has the given suffix.
func hasSuffix(path, suffix string) bool {
	return strings.HasSuffix(strings.ToLower(path), suffix)
}

// isVCFGz tells you if path is named like a vcf.gz file.
func isVCFGz(path string) bool {
	return hasSuffix(path, ".vcf.gz")
}

// isBCF tells you if path is named like a bcf file.
func isBCF(path string) bool {
	return hasSuffix(path, ".bcf")
}

// isSam tells you if path is named like a sam file.
func isSam(path string) bool {
	return hasSuffix(path, ".sam")
}

// isBam tells you if path is named like a bam file.
func isBam(path string) bool {
	return hasSuffix(path, ".bam")
}

// isCram tells you if path is named like a cram file.
func isCram(path string) bool {
	return hasSuffix(path, ".cram")
}

// isFasta tells you if path is named like a fasta file.
func isFasta(path string) bool {
	return hasOneOfSuffixes(path, []string{".fasta", ".fa"})
}

// isFastq tells you if path is named like a fastq file.
func isFastq(path string) bool {
	return hasOneOfSuffixes(path, []string{".fastq", ".fq"})
}

// isFastqGz tells you if path is named like a fastq.gz file.
func isFastqGz(path string) bool {
	return hasOneOfSuffixes(path, []string{".fastq.gz", ".fq.gz"})
}

// isPedBed tells you if path is named like a ped/bed file.
func isPedBed(path string) bool {
	return hasOneOfSuffixes(path, []string{".ped", ".map", ".bed", ".bim", ".fam"})
}

// isCompressed tells you if path is named like a compressed file.
func isCompressed(path string) bool {
	if isFastqGz(path) || isVCFGz(path) {
		return false
	}

	return hasOneOfSuffixes(path, []string{".bzip2", ".gz", ".tgz", ".zip", ".xz", ".bgz"})
}

// isText tells you if path is named like some standard text file.
func isText(path string) bool {
	return hasOneOfSuffixes(path, []string{".csv", ".tsv", ".txt", ".text", ".md", ".dat", "readme"})
}

// isLog tells you if path is named like some standard log file.
func isLog(path string) bool {
	return hasOneOfSuffixes(path, []string{".log", ".out", ".o", ".err", ".e", ".oe"})
}

// Add is a github.com/wtsi-ssg/wrstat/stat Operation. It will break path in to
// its directories and add the file size, increment the file count to each,
// summed for the info's group, user and filetype. It will also record the
// oldest file access time for each directory, plus the newest modification
// time.
//
// If path is a directory, its access time is treated as now, so that when
// interested in files that haven't been accessed in a long time, directories
// that haven't been manually visted in a longer time don't hide the "real"
// results.
//
// "Access" times are actually considered to be the greatest of atime, mtime and
// ctime.
//
// NB: the "temp" filetype is an extra filetype on top of the other normal
// filetypes, so if you sum all the filetypes to get information about a a given
// directory+group+user combination, you should ignore "temp". Only count "temp"
// when it's the only type you're considering, or you'll count some files twice.
func (d *DirGroupUserType) Add(path string, info fs.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errNotUnix
	}

	var atime int64

	if info.IsDir() {
		atime = time.Now().Unix()
	} else {
		atime = maxInt(stat.Ctim.Sec, stat.Mtim.Sec, stat.Atim.Sec)
	}

	d.addForEachDir(path, d.statToGUTKeys(stat, path), info.Size(), atime, stat.Mtim.Sec)

	return nil
}

// maxInt returns the greatest of the inputs.
func maxInt(ints ...int64) int64 {
	var max int64

	for _, i := range ints {
		if i > max {
			max = i
		}
	}

	return max
}

// statToGUTKey extracts gid and uid from the stat, determines the filetype from
// the path, and combines them into a group+user+type key. More than 1 key can
// be returned, because there is a "temp" filetype as well as more specific
// types, and path could be both.
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

	if len(types) == 0 || (len(types) == 1 && types[0] == DGUTFileTypeTemp) {
		types = append(types, DGUTFileTypeOther)
	}

	return types
}

// addForEachDir breaks path into each directory, gets a gutStore for each and
// adds a file of the given size to them under the given gutKeys.
func (d *DirGroupUserType) addForEachDir(path string, gutKeys []string, size int64, atime int64, mtime int64) {
	cb := func(dir string) {
		gStore := d.store.getGUTStore(dir)

		for _, gutKey := range gutKeys {
			gStore.add(gutKey, size, atime, mtime)
		}
	}

	doForEachDir(path, cb)
}

// Output will write summary information for all the paths previously added. The
// format is (tab separated):
//
// directory gid uid filetype filecount filesize atime mtime
//
// Where atime is oldest access time in seconds since Unix epoch of any file
// nested within directory. mtime is similar, but the newest modification time.
//
// directory, gid, uid and filetype are sorted. The sort on the columns is not
// numeric, but alphabetical. So gid 10 will come before gid 2.
//
// filetype is one of our filetype ints:
//
//	 0 = other (not any of the others below)
//	 1 = temp (.tmp | temp suffix, or .tmp. | .temp. | tmp. | temp. prefix, or
//	           a directory in its path is named "tmp" or "temp")
//	 2 = vcf
//	 3 = vcf.gz
//	 4 = bcf
//	 5 = sam
//	 6 = bam
//	 7 = cram
//	 8 = fasta (.fa | .fasta suffix)
//	 9 = fastq (.fq | .fastq suffix)
//	10 = fastq.gz (.fq.gz | .fastq.gz suffix)
//	11 = ped/bed (.ped | .map | .bed | .bim | .fam suffix)
//	12 = compresed (.bzip2 | .gz | .tgz | .zip | .xz | .bgz suffix)
//	13 = text (.csv | .tsv | .txt | .text | .md | .dat | readme suffix)
//	14 = log (.log | .out | .o | .err | .e | .err | .oe suffix)
//
// Returns an error on failure to write. output is closed on completion.
func (d *DirGroupUserType) Output(output *os.File) error {
	dirs, gStores := d.store.sort()

	for i, dir := range dirs {
		dguts, summaries := gStores[i].sort()

		for j, dgut := range dguts {
			s := summaries[j]
			_, errw := output.WriteString(fmt.Sprintf("%s\t%s\t%d\t%d\t%d\t%d\n", dir, dgut, s.count, s.size, s.atime, s.mtime))

			if errw != nil {
				return errw
			}
		}
	}

	return output.Close()
}
