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
	"io"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/wtsi-ssg/wrstat/v5/internal/encode"
)

// DirGUTAge is one of the age types that the
// directory,group,user,filetype,age summaries group on. All is for files of
// all ages. The AgeA* consider age according to access time. The AgeM* consider
// age according to modify time. The *\dM ones are age in the number of months,
// and the *\dY ones are in number of years.
type DirGUTAge uint8

const (
	DGUTAgeAll DirGUTAge = 0
	DGUTAgeA1M DirGUTAge = 1
	DGUTAgeA2M DirGUTAge = 2
	DGUTAgeA6M DirGUTAge = 3
	DGUTAgeA1Y DirGUTAge = 4
	DGUTAgeA2Y DirGUTAge = 5
	DGUTAgeA3Y DirGUTAge = 6
	DGUTAgeA5Y DirGUTAge = 7
	DGUTAgeA7Y DirGUTAge = 8
	DGUTAgeM1M DirGUTAge = 9
	DGUTAgeM2M DirGUTAge = 10
	DGUTAgeM6M DirGUTAge = 11
	DGUTAgeM1Y DirGUTAge = 12
	DGUTAgeM2Y DirGUTAge = 13
	DGUTAgeM3Y DirGUTAge = 14
	DGUTAgeM5Y DirGUTAge = 15
	DGUTAgeM7Y DirGUTAge = 16
)

var dirGUTAges = [17]DirGUTAge{ //nolint:gochecknoglobals
	DGUTAgeAll, DGUTAgeA1M, DGUTAgeA2M, DGUTAgeA6M, DGUTAgeA1Y,
	DGUTAgeA2Y, DGUTAgeA3Y, DGUTAgeA5Y, DGUTAgeA7Y, DGUTAgeM1M,
	DGUTAgeM2M, DGUTAgeM6M, DGUTAgeM1Y, DGUTAgeM2Y, DGUTAgeM3Y,
	DGUTAgeM5Y, DGUTAgeM7Y,
}

// DirGUTAFileType is one of the special file types that the
// directory,group,user,filetype,age summaries group on.
type DirGUTAFileType uint8

const (
	DGUTAFileTypeOther      DirGUTAFileType = 0
	DGUTAFileTypeTemp       DirGUTAFileType = 1
	DGUTAFileTypeVCF        DirGUTAFileType = 2
	DGUTAFileTypeVCFGz      DirGUTAFileType = 3
	DGUTAFileTypeBCF        DirGUTAFileType = 4
	DGUTAFileTypeSam        DirGUTAFileType = 5
	DGUTAFileTypeBam        DirGUTAFileType = 6
	DGUTAFileTypeCram       DirGUTAFileType = 7
	DGUTAFileTypeFasta      DirGUTAFileType = 8
	DGUTAFileTypeFastq      DirGUTAFileType = 9
	DGUTAFileTypeFastqGz    DirGUTAFileType = 10
	DGUTAFileTypePedBed     DirGUTAFileType = 11
	DGUTAFileTypeCompressed DirGUTAFileType = 12
	DGUTAFileTypeText       DirGUTAFileType = 13
	DGUTAFileTypeLog        DirGUTAFileType = 14
	DGUTAFileTypeDir        DirGUTAFileType = 15
)

var AllTypesExceptDirectories = []DirGUTAFileType{ //nolint:gochecknoglobals
	DGUTAFileTypeOther,
	DGUTAFileTypeTemp,
	DGUTAFileTypeVCF,
	DGUTAFileTypeVCFGz,
	DGUTAFileTypeBCF,
	DGUTAFileTypeSam,
	DGUTAFileTypeBam,
	DGUTAFileTypeCram,
	DGUTAFileTypeFasta,
	DGUTAFileTypeFastq,
	DGUTAFileTypeFastqGz,
	DGUTAFileTypePedBed,
	DGUTAFileTypeCompressed,
	DGUTAFileTypeText,
	DGUTAFileTypeLog,
}

const ErrInvalidType = Error("not a valid file type")

// String lets you convert a DirGUTAFileType to a meaningful string.
func (d DirGUTAFileType) String() string {
	return [...]string{"other", "temp", "vcf", "vcf.gz", "bcf", "sam", "bam",
		"cram", "fasta", "fastq", "fastq.gz", "ped/bed", "compressed", "text",
		"log", "dir"}[d]
}

// FileTypeStringToDirGUTAFileType converts the String() representation of a
// DirGUTAFileType back in to a DirGUTAFileType. Errors if an invalid string
// supplied.
func FileTypeStringToDirGUTAFileType(ft string) (DirGUTAFileType, error) {
	convert := map[string]DirGUTAFileType{
		"other":      DGUTAFileTypeOther,
		"temp":       DGUTAFileTypeTemp,
		"vcf":        DGUTAFileTypeVCF,
		"vcf.gz":     DGUTAFileTypeVCFGz,
		"bcf":        DGUTAFileTypeBCF,
		"sam":        DGUTAFileTypeSam,
		"bam":        DGUTAFileTypeBam,
		"cram":       DGUTAFileTypeCram,
		"fasta":      DGUTAFileTypeFasta,
		"fastq":      DGUTAFileTypeFastq,
		"fastq.gz":   DGUTAFileTypeFastqGz,
		"ped/bed":    DGUTAFileTypePedBed,
		"compressed": DGUTAFileTypeCompressed,
		"text":       DGUTAFileTypeText,
		"log":        DGUTAFileTypeLog,
		"dir":        DGUTAFileTypeDir,
	}

	dgft, ok := convert[ft]

	if !ok {
		return DGUTAFileTypeOther, ErrInvalidType
	}

	return dgft, nil
}

// gutaStore is a sortable map with gid,uid,filetype,age as keys and
// summaryWithAtime as values.
type gutaStore struct {
	sumMap  map[string]*summaryWithTimes
	refTime int64
}

// add will auto-vivify a summary for the given key (which should have been
// generated with statToGUTAKey()) and call add(size, atime, mtime) on it.
func (store gutaStore) add(key string, size int64, atime int64, mtime int64) {
	s, ok := store.sumMap[key]
	if !ok {
		s = &summaryWithTimes{refTime: store.refTime}
		store.sumMap[key] = s
	}

	if !s.fitsAgeInterval(key, atime, mtime) {
		return
	}

	s.add(size, atime, mtime)
}

// sort returns a slice of our summaryWithAtime values, sorted by our dguta keys
// which are also returned.
func (store gutaStore) sort() ([]string, []*summaryWithTimes) {
	return sortSummaryStore(store.sumMap)
}

// dirToGUTAStore is a sortable map of directory to gutaStore.
type dirToGUTAStore struct {
	gsMap   map[string]gutaStore
	refTime int64
}

// getGUTAStore auto-vivifies a gutaStore for the given dir and returns it.
func (store dirToGUTAStore) getGUTAStore(dir string) gutaStore {
	gStore, ok := store.gsMap[dir]
	if !ok {
		gStore = gutaStore{make(map[string]*summaryWithTimes), store.refTime}
		store.gsMap[dir] = gStore
	}

	return gStore
}

// sort returns a slice of our gutaStore values, sorted by our directory keys
// which are also returned.
func (store dirToGUTAStore) sort() ([]string, []gutaStore) {
	keys := make([]string, len(store.gsMap))
	i := 0

	for k := range store.gsMap {
		keys[i] = k
		i++
	}

	sort.Strings(keys)

	s := make([]gutaStore, len(keys))

	for i, k := range keys {
		s[i] = store.gsMap[k]
	}

	return keys, s
}

// typeCheckers take a path and return true if the path is of their file type.
type typeChecker func(path string) bool

// DirGroupUserTypeAge is used to summarise file stats by directory, group,
// user, file type and age.
type DirGroupUserTypeAge struct {
	store        dirToGUTAStore
	typeCheckers map[DirGUTAFileType]typeChecker
}

// NewDirGroupUserTypeAge returns a DirGroupUserTypeAge.
func NewDirGroupUserTypeAge() *DirGroupUserTypeAge {
	return &DirGroupUserTypeAge{
		store: dirToGUTAStore{make(map[string]gutaStore), time.Now().Unix()},
		typeCheckers: map[DirGUTAFileType]typeChecker{
			DGUTAFileTypeTemp:       isTemp,
			DGUTAFileTypeVCF:        isVCF,
			DGUTAFileTypeVCFGz:      isVCFGz,
			DGUTAFileTypeBCF:        isBCF,
			DGUTAFileTypeSam:        isSam,
			DGUTAFileTypeBam:        isBam,
			DGUTAFileTypeCram:       isCram,
			DGUTAFileTypeFasta:      isFasta,
			DGUTAFileTypeFastq:      isFastq,
			DGUTAFileTypeFastqGz:    isFastqGz,
			DGUTAFileTypePedBed:     isPedBed,
			DGUTAFileTypeCompressed: isCompressed,
			DGUTAFileTypeText:       isText,
			DGUTAFileTypeLog:        isLog,
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
// summed for the info's group, user, filetype and age. It will also record the
// oldest file access time for each directory, plus the newest modification
// time.
//
// If path is a directory, its access time is treated as now, so that when
// interested in files that haven't been accessed in a long time, directories
// that haven't been manually visted in a longer time don't hide the "real"
// results.
//
// "Access" times are actually considered to be the greatest of atime, mtime and
// unix epoch.
//
// NB: the "temp" filetype is an extra filetype on top of the other normal
// filetypes, so if you sum all the filetypes to get information about a given
// directory+group+user combination, you should ignore "temp". Only count "temp"
// when it's the only type you're considering, or you'll count some files twice.
func (d *DirGroupUserTypeAge) Add(path string, info fs.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errNotUnix
	}

	var atime int64

	var gutaKeys []string

	if info.IsDir() {
		atime = time.Now().Unix()
		path = filepath.Join(path, "leaf")

		if isTemp(path) {
			gutaKeys = appendGUTAKeys(gutaKeys, stat.Gid, stat.Uid, DGUTAFileTypeTemp)
		}

		gutaKeys = appendGUTAKeys(gutaKeys, stat.Gid, stat.Uid, DGUTAFileTypeDir)
	} else {
		atime = maxInt(0, stat.Mtim.Sec, stat.Atim.Sec)
		gutaKeys = d.statToGUTAKeys(stat, path)
	}

	d.addForEachDir(path, gutaKeys, info.Size(), atime, maxInt(0, stat.Mtim.Sec))

	return nil
}

// appendGUTAKeys appends gutaKeys with keys including the given gid, uid, file
// type and age.
func appendGUTAKeys(gutaKeys []string, gid, uid uint32, fileType DirGUTAFileType) []string {
	for _, age := range dirGUTAges {
		gutaKeys = append(gutaKeys, fmt.Sprintf("%d\t%d\t%d\t%d", gid, uid, fileType, age))
	}

	return gutaKeys
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

// statToGUTAKeys extracts gid and uid from the stat, determines the filetype
// from the path, and combines them into a group+user+type+age key. More than 1
// key will be returned, because there is a key for each age, possibly a "temp"
// filetype as well as more specific types, and path could be both.
func (d *DirGroupUserTypeAge) statToGUTAKeys(stat *syscall.Stat_t, path string) []string {
	types := d.pathToTypes(path)
	gutaKeys := make([]string, 0, len(dirGUTAges)*len(types))

	for _, t := range types {
		gutaKeys = appendGUTAKeys(gutaKeys, stat.Gid, stat.Uid, t)
	}

	return gutaKeys
}

// pathToTypes determines the filetype of the given path based on its basename,
// and returns a slice of our DirGUTAFileType. More than one is possible,
// because a path can be both a temporary file, and another type.
func (d *DirGroupUserTypeAge) pathToTypes(path string) []DirGUTAFileType {
	var types []DirGUTAFileType

	for ftype, isThisType := range d.typeCheckers {
		if isThisType(path) {
			types = append(types, ftype)
		}
	}

	if len(types) == 0 || (len(types) == 1 && types[0] == DGUTAFileTypeTemp) {
		types = append(types, DGUTAFileTypeOther)
	}

	return types
}

// addForEachDir breaks path into each directory, gets a gutaStore for each and
// adds a file of the given size to them under the given gutaKeys.
func (d *DirGroupUserTypeAge) addForEachDir(path string, gutaKeys []string, size int64, atime int64, mtime int64) {
	cb := func(dir string) {
		gStore := d.store.getGUTAStore(dir)

		for _, gutaKey := range gutaKeys {
			gStore.add(gutaKey, size, atime, mtime)
		}
	}

	doForEachDir(path, cb)
}

type StringCloser interface {
	io.StringWriter
	io.Closer
}

// Output will write summary information for all the paths previously added. The
// format is (tab separated):
//
// directory gid uid filetype age filecount filesize atime mtime
//
// Where atime is oldest access time in seconds since Unix epoch of any file
// nested within directory. mtime is similar, but the newest modification time.
//
// age is one of our age ints:
//
//		 0 = all ages
//		 1 = older than one month according to atime
//		 2 = older than two months according to atime
//		 3 = older than six months according to atime
//		 4 = older than one year according to atime
//		 5 = older than two years according to atime
//		 6 = older than three years according to atime
//		 7 = older than five years according to atime
//		 8 = older than seven years according to atime
//		 9 = older than one month according to mtime
//		10 = older than two months according to mtime
//		11 = older than six months according to mtime
//		12 = older than one year according to mtime
//		13 = older than two years according to mtime
//		14 = older than three years according to mtime
//	 15 = older than five years according to mtime
//		16 = older than seven years according to mtime
//
// directory, gid, uid, filetype and age are sorted. The sort on the columns is
// not numeric, but alphabetical. So gid 10 will come before gid 2.
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
func (d *DirGroupUserTypeAge) Output(output StringCloser) error {
	dirs, gStores := d.store.sort()

	for i, dir := range dirs {
		dgutas, summaries := gStores[i].sort()

		for j, dguta := range dgutas {
			s := summaries[j]
			_, errw := output.WriteString(fmt.Sprintf("%s\t%s\t%d\t%d\t%d\t%d\n",
				encode.Base64Encode(dir),
				dguta,
				s.count, s.size,
				s.atime, s.mtime))

			if errw != nil {
				return errw
			}
		}
	}

	return output.Close()
}
