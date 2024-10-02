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
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/v5/internal/encode"
)

func TestDirGUTFileType(t *testing.T) {
	Convey("DGUTFileType* consts are ints that can be stringified", t, func() {
		So(DirGUTFileType(0).String(), ShouldEqual, "other")
		So(DGUTFileTypeOther.String(), ShouldEqual, "other")
		So(DGUTFileTypeTemp.String(), ShouldEqual, "temp")
		So(DGUTFileTypeVCF.String(), ShouldEqual, "vcf")
		So(DGUTFileTypeVCFGz.String(), ShouldEqual, "vcf.gz")
		So(DGUTFileTypeBCF.String(), ShouldEqual, "bcf")
		So(DGUTFileTypeSam.String(), ShouldEqual, "sam")
		So(DGUTFileTypeBam.String(), ShouldEqual, "bam")
		So(DGUTFileTypeCram.String(), ShouldEqual, "cram")
		So(DGUTFileTypeFasta.String(), ShouldEqual, "fasta")
		So(DGUTFileTypeFastq.String(), ShouldEqual, "fastq")
		So(DGUTFileTypeFastqGz.String(), ShouldEqual, "fastq.gz")
		So(DGUTFileTypePedBed.String(), ShouldEqual, "ped/bed")
		So(DGUTFileTypeCompressed.String(), ShouldEqual, "compressed")
		So(DGUTFileTypeText.String(), ShouldEqual, "text")
		So(DGUTFileTypeLog.String(), ShouldEqual, "log")

		So(int(DGUTFileTypeTemp), ShouldEqual, 1)
	})

	Convey("You can go from a string to a DGUTFileType", t, func() {
		ft, err := FileTypeStringToDirGUTFileType("other")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypeOther)

		ft, err = FileTypeStringToDirGUTFileType("temp")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypeTemp)

		ft, err = FileTypeStringToDirGUTFileType("vcf")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypeVCF)

		ft, err = FileTypeStringToDirGUTFileType("vcf.gz")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypeVCFGz)

		ft, err = FileTypeStringToDirGUTFileType("bcf")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypeBCF)

		ft, err = FileTypeStringToDirGUTFileType("sam")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypeSam)

		ft, err = FileTypeStringToDirGUTFileType("bam")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypeBam)

		ft, err = FileTypeStringToDirGUTFileType("cram")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypeCram)

		ft, err = FileTypeStringToDirGUTFileType("fasta")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypeFasta)

		ft, err = FileTypeStringToDirGUTFileType("fastq")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypeFastq)

		ft, err = FileTypeStringToDirGUTFileType("fastq.gz")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypeFastqGz)

		ft, err = FileTypeStringToDirGUTFileType("ped/bed")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypePedBed)

		ft, err = FileTypeStringToDirGUTFileType("compressed")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypeCompressed)

		ft, err = FileTypeStringToDirGUTFileType("text")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypeText)

		ft, err = FileTypeStringToDirGUTFileType("log")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypeLog)

		ft, err = FileTypeStringToDirGUTFileType("foo")
		So(err, ShouldNotBeNil)
		So(err, ShouldEqual, ErrInvalidType)
		So(ft, ShouldEqual, DGUTFileTypeOther)
	})

	Convey("isTemp lets you know if a path is a temporary file", t, func() {
		So(isTemp("/foo/.tmp.cram"), ShouldBeTrue)
		So(isTemp("/foo/tmp.cram"), ShouldBeTrue)
		So(isTemp("/foo/xtmp.cram"), ShouldBeFalse)
		So(isTemp("/foo/tmpx.cram"), ShouldBeFalse)

		So(isTemp("/foo/.temp.cram"), ShouldBeTrue)
		So(isTemp("/foo/temp.cram"), ShouldBeTrue)
		So(isTemp("/foo/xtemp.cram"), ShouldBeFalse)
		So(isTemp("/foo/tempx.cram"), ShouldBeFalse)

		So(isTemp("/foo/a.cram.tmp"), ShouldBeTrue)
		So(isTemp("/foo/xtmp"), ShouldBeFalse)
		So(isTemp("/foo/a.cram.temp"), ShouldBeTrue)
		So(isTemp("/foo/xtemp"), ShouldBeFalse)

		So(isTemp("/foo/tmp/bar.cram"), ShouldBeTrue)
		So(isTemp("/foo/temp/bar.cram"), ShouldBeTrue)
		So(isTemp("/foo/TEMP/bar.cram"), ShouldBeTrue)
		So(isTemp("/foo/bar.cram"), ShouldBeFalse)
	})

	Convey("isVCF lets you know if a path is a vcf file", t, func() {
		So(isVCF("/foo/bar.vcf"), ShouldBeTrue)
		So(isVCF("/foo/bar.VCF"), ShouldBeTrue)
		So(isVCF("/foo/vcf.bar"), ShouldBeFalse)
		So(isVCF("/foo/bar.fcv"), ShouldBeFalse)
	})

	Convey("isVCFGz lets you know if a path is a vcf.gz file", t, func() {
		So(isVCFGz("/foo/bar.vcf.gz"), ShouldBeTrue)
		So(isVCFGz("/foo/vcf.gz.bar"), ShouldBeFalse)
		So(isVCFGz("/foo/bar.vcf"), ShouldBeFalse)
	})

	Convey("isBCF lets you know if a path is a bcf file", t, func() {
		So(isBCF("/foo/bar.bcf"), ShouldBeTrue)
		So(isBCF("/foo/bcf.bar"), ShouldBeFalse)
		So(isBCF("/foo/bar.vcf"), ShouldBeFalse)
	})

	Convey("isSam lets you know if a path is a sam file", t, func() {
		So(isSam("/foo/bar.sam"), ShouldBeTrue)
		So(isSam("/foo/bar.bam"), ShouldBeFalse)
	})

	Convey("isBam lets you know if a path is a bam file", t, func() {
		So(isBam("/foo/bar.bam"), ShouldBeTrue)
		So(isBam("/foo/bar.sam"), ShouldBeFalse)
	})

	Convey("isCram lets you know if a path is a cram file", t, func() {
		So(isCram("/foo/bar.cram"), ShouldBeTrue)
		So(isCram("/foo/bar.bam"), ShouldBeFalse)
	})

	Convey("isFasta lets you know if a path is a fasta file", t, func() {
		So(isFasta("/foo/bar.fasta"), ShouldBeTrue)
		So(isFasta("/foo/bar.fa"), ShouldBeTrue)
		So(isFasta("/foo/bar.fastq"), ShouldBeFalse)
	})

	Convey("isFastq lets you know if a path is a fastq file", t, func() {
		So(isFastq("/foo/bar.fastq"), ShouldBeTrue)
		So(isFastq("/foo/bar.fq"), ShouldBeTrue)
		So(isFastq("/foo/bar.fasta"), ShouldBeFalse)
		So(isFastq("/foo/bar.fastq.gz"), ShouldBeFalse)
	})

	Convey("isFastqGz lets you know if a path is a fastq.gz file", t, func() {
		So(isFastqGz("/foo/bar.fastq.gz"), ShouldBeTrue)
		So(isFastqGz("/foo/bar.fq.gz"), ShouldBeTrue)
		So(isFastqGz("/foo/bar.fastq"), ShouldBeFalse)
		So(isFastqGz("/foo/bar.fq"), ShouldBeFalse)
	})

	Convey("isPedBed lets you know if a path is a ped/bed related file", t, func() {
		So(isPedBed("/foo/bar.ped"), ShouldBeTrue)
		So(isPedBed("/foo/bar.map"), ShouldBeTrue)
		So(isPedBed("/foo/bar.bed"), ShouldBeTrue)
		So(isPedBed("/foo/bar.bim"), ShouldBeTrue)
		So(isPedBed("/foo/bar.fam"), ShouldBeTrue)
		So(isPedBed("/foo/bar.asd"), ShouldBeFalse)
	})

	Convey("isCompressed lets you know if a path is a compressed file", t, func() {
		So(isCompressed("/foo/bar.bzip2"), ShouldBeTrue)
		So(isCompressed("/foo/bar.gz"), ShouldBeTrue)
		So(isCompressed("/foo/bar.tgz"), ShouldBeTrue)
		So(isCompressed("/foo/bar.zip"), ShouldBeTrue)
		So(isCompressed("/foo/bar.xz"), ShouldBeTrue)
		So(isCompressed("/foo/bar.bgz"), ShouldBeTrue)
		So(isCompressed("/foo/bar.bcf"), ShouldBeFalse)
		So(isCompressed("/foo/bar.asd"), ShouldBeFalse)
		So(isCompressed("/foo/bar.vcf.gz"), ShouldBeFalse)
		So(isCompressed("/foo/bar.fastq.gz"), ShouldBeFalse)
	})

	Convey("isText lets you know if a path is a text file", t, func() {
		So(isText("/foo/bar.csv"), ShouldBeTrue)
		So(isText("/foo/bar.tsv"), ShouldBeTrue)
		So(isText("/foo/bar.txt"), ShouldBeTrue)
		So(isText("/foo/bar.text"), ShouldBeTrue)
		So(isText("/foo/bar.md"), ShouldBeTrue)
		So(isText("/foo/bar.dat"), ShouldBeTrue)
		So(isText("/foo/bar.README"), ShouldBeTrue)
		So(isText("/foo/READme"), ShouldBeTrue)
		So(isText("/foo/bar.sam"), ShouldBeFalse)
		So(isText("/foo/bar.out"), ShouldBeFalse)
		So(isText("/foo/bar.asd"), ShouldBeFalse)
	})

	Convey("isLog lets you know if a path is a log file", t, func() {
		So(isLog("/foo/bar.log"), ShouldBeTrue)
		So(isLog("/foo/bar.o"), ShouldBeTrue)
		So(isLog("/foo/bar.out"), ShouldBeTrue)
		So(isLog("/foo/bar.e"), ShouldBeTrue)
		So(isLog("/foo/bar.err"), ShouldBeTrue)
		So(isLog("/foo/bar.oe"), ShouldBeTrue)
		So(isLog("/foo/bar.txt"), ShouldBeFalse)
		So(isLog("/foo/bar.asd"), ShouldBeFalse)
	})

	Convey("DirGroupUserType.pathToTypes lets you know the filetypes of a path", t, func() {
		d := NewByDirGroupUserType()

		So(d.pathToTypes("/foo/bar.asd"), ShouldResemble, []DirGUTFileType{DGUTFileTypeOther})
		So(pathToTypesMap(d, "/foo/.tmp.asd"), ShouldResemble, map[DirGUTFileType]bool{
			DGUTFileTypeOther: true, DGUTFileTypeTemp: true})

		So(d.pathToTypes("/foo/bar.vcf"), ShouldResemble, []DirGUTFileType{DGUTFileTypeVCF})
		So(d.pathToTypes("/foo/bar.vcf.gz"), ShouldResemble, []DirGUTFileType{DGUTFileTypeVCFGz})
		So(d.pathToTypes("/foo/bar.bcf"), ShouldResemble, []DirGUTFileType{DGUTFileTypeBCF})

		So(d.pathToTypes("/foo/bar.sam"), ShouldResemble, []DirGUTFileType{DGUTFileTypeSam})
		So(d.pathToTypes("/foo/bar.bam"), ShouldResemble, []DirGUTFileType{DGUTFileTypeBam})
		So(pathToTypesMap(d, "/foo/.tmp.cram"), ShouldResemble, map[DirGUTFileType]bool{
			DGUTFileTypeCram: true, DGUTFileTypeTemp: true})

		So(d.pathToTypes("/foo/bar.fa"), ShouldResemble, []DirGUTFileType{DGUTFileTypeFasta})
		So(d.pathToTypes("/foo/bar.fq"), ShouldResemble, []DirGUTFileType{DGUTFileTypeFastq})
		So(d.pathToTypes("/foo/bar.fq.gz"), ShouldResemble, []DirGUTFileType{DGUTFileTypeFastqGz})

		So(d.pathToTypes("/foo/bar.bzip2"), ShouldResemble, []DirGUTFileType{DGUTFileTypeCompressed})
		So(d.pathToTypes("/foo/bar.csv"), ShouldResemble, []DirGUTFileType{DGUTFileTypeText})
		So(d.pathToTypes("/foo/bar.o"), ShouldResemble, []DirGUTFileType{DGUTFileTypeLog})
	})
}

// pathToTypesMap is used in tests to help ignore the order of types returned by
// DirGroupUserType.pathToTypes, for test comparison purposes.
func pathToTypesMap(d *DirGroupUserType, path string) map[DirGUTFileType]bool {
	types := d.pathToTypes(path)
	m := make(map[DirGUTFileType]bool, len(types))

	for _, ftype := range types {
		m[ftype] = true
	}

	return m
}

func TestDirGUT(t *testing.T) {
	usr, err := user.Current()
	if err != nil {
		t.Fatal(err.Error())
	}

	cuidI, err := strconv.Atoi(usr.Uid)
	if err != nil {
		t.Fatal(err.Error())
	}

	cuid := uint32(cuidI)

	Convey("Given a Usergroup", t, func() {
		dgut := NewByDirGroupUserType()
		So(dgut, ShouldNotBeNil)

		Convey("You can add file info with a range of Atimes to it", func() {
			atime1 := time.Now().Unix() - (secondsIn2m + 100000)      // > 2 m
			mtime1 := time.Now().Unix() - (secondsIn2m + secondsIn1m) // 3 m
			mi := newMockInfoWithAtime(10, 2, 2, false, atime1)
			mi.mtime = mtime1
			err = dgut.Add("/a/b/c/1.bam", mi)
			So(err, ShouldBeNil)

			atime2 := time.Now().Unix() - (secondsIn6m + secondsIn1m) // 7 m
			mtime2 := time.Now().Unix() - (secondsIn6m + secondsIn2m) // 8 m
			mi = newMockInfoWithAtime(10, 2, 3, false, atime2)
			mi.mtime = mtime2
			err = dgut.Add("/a/b/c/2.bam", mi)
			So(err, ShouldBeNil)

			atime3 := time.Now().Unix() - (secondsIn1y + secondsIn1m) // 1 y 1 m
			mtime3 := time.Now().Unix() - (secondsIn1y + secondsIn6m) // 1 y 6 m
			mi = newMockInfoWithAtime(10, 2, 4, false, atime3)
			mi.mtime = mtime3
			err = dgut.Add("/a/b/c/3.txt", mi)
			So(err, ShouldBeNil)

			atime4 := time.Now().Unix() - (secondsIn3y + secondsIn1y) // 4 y
			mtime4 := time.Now().Unix() - (secondsIn3y + secondsIn3y) // 6 y
			mi = newMockInfoWithAtime(10, 2, 5, false, atime4)
			mi.mtime = mtime4
			err = dgut.Add("/a/b/c/4.bam", mi)
			So(err, ShouldBeNil)

			atime5 := time.Now().Unix() - (secondsIn5y + secondsIn1m) // 5 y 1 m
			mtime5 := time.Now().Unix() - (secondsIn7y + secondsIn1m) // 7 y 1 m
			mi = newMockInfoWithAtime(10, 2, 6, false, atime5)
			mi.mtime = mtime5
			err = dgut.Add("/a/b/c/5.cram", mi)
			So(err, ShouldBeNil)

			atime6 := time.Now().Unix() - (secondsIn7y + secondsIn1m) // 7 y 1 m
			mtime6 := time.Now().Unix() - (secondsIn7y + secondsIn1m) // 7 y 1 m
			mi = newMockInfoWithAtime(10, 2, 7, false, atime6)
			mi.mtime = mtime6
			err = dgut.Add("/a/b/c/6.cram", mi)
			So(err, ShouldBeNil)

			So(dgut.store["/a/b/c"]["2\t10\t6"], ShouldResemble, &summaryWithTimes{summary{3, 10},
				atime4, mtime1,
				0, 0, 5, 5, 5, 8, 10, 10,
				0, 5, 5, 5, 5, 8, 10, 10})
			So(dgut.store["/a/b/c"]["2\t10\t7"], ShouldResemble, &summaryWithTimes{summary{2, 13},
				atime6, mtime5,
				7, 13, 13, 13, 13, 13, 13, 13,
				13, 13, 13, 13, 13, 13, 13, 13})
			So(dgut.store["/a/b/c"]["2\t10\t13"], ShouldResemble, &summaryWithTimes{summary{1, 4},
				atime3, mtime3,
				0, 0, 0, 0, 4, 4, 4, 4,
				0, 0, 0, 0, 4, 4, 4, 4})

			Convey("And then given an output file", func() {
				dir := t.TempDir()
				outPath := filepath.Join(dir, "out")
				out, errc := os.Create(outPath)
				So(errc, ShouldBeNil)

				Convey("You can output the summaries to file", func() {
					err = dgut.Output(out)
					So(err, ShouldBeNil)
					err = out.Close()
					So(err, ShouldNotBeNil)

					o, errr := os.ReadFile(outPath)
					So(errr, ShouldBeNil)

					output := string(o)

					So(output, ShouldContainSubstring, encode.Base64Encode("/a/b/c")+
						"\t2\t10\t6\t3\t10\t"+strconv.Itoa(int(atime4))+"\t"+strconv.Itoa(int(mtime1))+"\t0\t0\t5\t5\t5\t8\t10\t10\t0\t5\t5\t5\t5\t8\t10\t10\n")
					So(output, ShouldContainSubstring, encode.Base64Encode("/a/b/c")+
						"\t2\t10\t7\t2\t13\t"+strconv.Itoa(int(atime6))+"\t"+strconv.Itoa(int(mtime5))+"\t7\t13\t13\t13\t13\t13\t13\t13\t13\t13\t13\t13\t13\t13\t13\t13\n")
					So(output, ShouldContainSubstring, encode.Base64Encode("/a/b/c")+
						"\t2\t10\t13\t1\t4\t"+strconv.Itoa(int(atime3))+"\t"+strconv.Itoa(int(mtime3))+"\t0\t0\t0\t0\t4\t4\t4\t4\t0\t0\t0\t0\t4\t4\t4\t4\n")

					So(checkFileIsSorted(outPath), ShouldBeTrue)
				})
			})
		})

		Convey("You can add file info to it which accumulates the info", func() {
			addTestData(dgut, cuid)

			err = dgut.Add("/a/b/c/3.bam", newMockInfoWithAtime(2, 2, 3, false, 100))
			So(err, ShouldBeNil)
			mi := newMockInfoWithAtime(10, 2, 2, false, 250)
			mi.mtime = 250
			err = dgut.Add("/a/b/c/7.cram", mi)
			So(err, ShouldBeNil)

			mi = newMockInfoWithAtime(10, 2, 2, false, 199)
			mi.mtime = 200
			err = dgut.Add("/a/b/c/d/9.cram", mi)
			So(err, ShouldBeNil)

			mi = newMockInfoWithAtime(2, 10, 2, false, 300)
			mi.ctime = 301
			err = dgut.Add("/a/b/c/8.cram", mi)
			So(err, ShouldBeNil)

			before := time.Now().Unix()
			err = dgut.Add("/a/b/c/d", newMockInfoWithAtime(10, 2, 4096, true, 50))
			So(err, ShouldBeNil)

			So(dgut.store["/a/b/c"], ShouldNotBeNil)
			So(dgut.store["/a/b"], ShouldNotBeNil)
			So(dgut.store["/a"], ShouldNotBeNil)
			So(dgut.store["/"], ShouldNotBeNil)
			So(dgut.store[""], ShouldBeNil)

			cuidKey := fmt.Sprintf("2\t%d\t13", cuid)
			So(dgut.store["/a/b/c"][cuidKey], ShouldResemble, &summaryWithTimes{summary{2, 30}, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
			So(dgut.store["/a/b/c"]["2\t2\t13"], ShouldResemble, &summaryWithTimes{summary{1, 5}, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
			So(dgut.store["/a/b/c"]["2\t2\t6"], ShouldResemble, &summaryWithTimes{summary{1, 3}, 100, 0,
				3, 3, 3, 3, 3, 3, 3, 3, 0, 0, 0, 0, 0, 0, 0, 0})
			So(dgut.store["/a/b/c"]["3\t2\t13"], ShouldResemble, &summaryWithTimes{summary{1, 6}, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
			So(dgut.store["/a/b/c"]["3\t2\t1"], ShouldBeNil)
			So(dgut.store["/a/b/c"]["2\t10\t7"], ShouldResemble, &summaryWithTimes{summary{2, 4}, 200, 250,
				4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4})
			So(dgut.store["/a/b/c/d"]["2\t10\t7"], ShouldResemble, &summaryWithTimes{summary{1, 2}, 200, 200,
				2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2})
			So(dgut.store["/a/b/c"]["10\t2\t7"], ShouldResemble, &summaryWithTimes{summary{1, 2}, 301, 0,
				2, 2, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0})

			swa := dgut.store["/a/b"]["2\t10\t15"]
			if swa.atime >= before {
				swa.atime = 18
			}

			So(swa, ShouldResemble, &summaryWithTimes{summary{1, 4096}, 18, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})

			swa = dgut.store["/a/b/c"]["2\t10\t15"]
			if swa.atime >= before {
				swa.atime = 18
			}

			So(swa, ShouldResemble, &summaryWithTimes{summary{1, 4096}, 18, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
			So(dgut.store["/a/b/c/d"]["2\t10\t15"], ShouldNotBeNil)

			So(dgut.store["/a/b"][cuidKey], ShouldResemble, &summaryWithTimes{summary{3, 60}, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
			So(dgut.store["/a/b"]["2\t2\t13"], ShouldResemble, &summaryWithTimes{summary{1, 5}, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
			So(dgut.store["/a/b"]["2\t2\t6"], ShouldResemble, &summaryWithTimes{summary{1, 3}, 100, 0,
				3, 3, 3, 3, 3, 3, 3, 3, 0, 0, 0, 0, 0, 0, 0, 0})
			So(dgut.store["/a/b"]["3\t2\t13"], ShouldResemble, &summaryWithTimes{summary{1, 6}, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})

			So(dgut.store["/a"][cuidKey], ShouldResemble, &summaryWithTimes{summary{3, 60}, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
			So(dgut.store["/a"]["2\t2\t13"], ShouldResemble, &summaryWithTimes{summary{1, 5}, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
			So(dgut.store["/a"]["2\t2\t6"], ShouldResemble, &summaryWithTimes{summary{1, 3}, 100, 0,
				3, 3, 3, 3, 3, 3, 3, 3, 0, 0, 0, 0, 0, 0, 0, 0})
			So(dgut.store["/a"]["3\t2\t13"], ShouldResemble, &summaryWithTimes{summary{1, 6}, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})

			So(dgut.store["/"][cuidKey], ShouldResemble, &summaryWithTimes{summary{3, 60}, 0, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
			So(dgut.store["/"]["2\t2\t13"], ShouldResemble, &summaryWithTimes{summary{1, 5}, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
			So(dgut.store["/"]["2\t2\t6"], ShouldResemble, &summaryWithTimes{summary{1, 3}, 100, 0,
				3, 3, 3, 3, 3, 3, 3, 3, 0, 0, 0, 0, 0, 0, 0, 0})
			So(dgut.store["/"]["3\t2\t13"], ShouldResemble, &summaryWithTimes{summary{1, 6}, 0, 0,
				0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})

			Convey("And then given an output file", func() {
				dir := t.TempDir()
				outPath := filepath.Join(dir, "out")
				out, err := os.Create(outPath)
				So(err, ShouldBeNil)

				Convey("You can output the summaries to file", func() {
					err = dgut.Output(out)
					So(err, ShouldBeNil)
					err = out.Close()
					So(err, ShouldNotBeNil)

					o, errr := os.ReadFile(outPath)
					So(errr, ShouldBeNil)
					output := string(o)

					So(output, ShouldContainSubstring, encode.Base64Encode("/a/b/c/d")+
						"\t2\t10\t7\t1\t2\t200\t200\t2\t2\t2\t2\t2\t2\t2\t2\t2\t2\t2\t2\t2\t2\t2\t2\n")
					So(output, ShouldContainSubstring, encode.Base64Encode("/a/b/c")+
						"\t"+cuidKey+"\t2\t30\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n")
					So(output, ShouldContainSubstring, encode.Base64Encode("/a/b")+
						"\t"+cuidKey+"\t3\t60\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n")
					So(output, ShouldContainSubstring, encode.Base64Encode("/a/b")+
						"\t2\t2\t13\t1\t5\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n")
					So(output, ShouldContainSubstring, encode.Base64Encode("/a/b")+
						"\t2\t2\t6\t1\t3\t100\t0\t3\t3\t3\t3\t3\t3\t3\t3\t0\t0\t0\t0\t0\t0\t0\t0\n")
					So(output, ShouldContainSubstring, encode.Base64Encode("/")+
						"\t3\t2\t13\t1\t6\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\t0\n")

					So(checkFileIsSorted(outPath), ShouldBeTrue)
				})

				Convey("Output fails if we can't write to the output file", func() {
					err = out.Close()
					So(err, ShouldBeNil)

					err = dgut.Output(out)
					So(err, ShouldNotBeNil)
				})
			})
		})

		Convey("You can't Add() on non-unix-like systems'", func() {
			err := dgut.Add("/a/b/c/1.txt", &badInfo{})
			So(err, ShouldNotBeNil)
		})
	})
}
