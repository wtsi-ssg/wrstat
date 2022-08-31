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
			So(dgut.store["/a/b/c"][cuidKey], ShouldResemble, &summaryWithAtime{summary{2, 30}, 0, 0})
			So(dgut.store["/a/b/c"]["2\t2\t13"], ShouldResemble, &summaryWithAtime{summary{1, 5}, 0, 0})
			So(dgut.store["/a/b/c"]["2\t2\t6"], ShouldResemble, &summaryWithAtime{summary{1, 3}, 100, 0})
			So(dgut.store["/a/b/c"]["3\t2\t13"], ShouldResemble, &summaryWithAtime{summary{1, 6}, 0, 0})
			So(dgut.store["/a/b/c"]["3\t2\t1"], ShouldBeNil)
			So(dgut.store["/a/b/c"]["2\t10\t7"], ShouldResemble, &summaryWithAtime{summary{2, 4}, 200, 250})
			So(dgut.store["/a/b/c/d"]["2\t10\t7"], ShouldResemble, &summaryWithAtime{summary{1, 2}, 200, 200})
			So(dgut.store["/a/b/c"]["10\t2\t7"], ShouldResemble, &summaryWithAtime{summary{1, 2}, 301, 0})

			swa := dgut.store["/a/b"]["2\t10\t0"]
			if swa.atime >= before {
				swa.atime = 18
			}
			So(swa, ShouldResemble, &summaryWithAtime{summary{1, 4096}, 18, 0})

			swa = dgut.store["/a/b/c"]["2\t10\t0"]
			if swa.atime >= before {
				swa.atime = 18
			}
			So(swa, ShouldResemble, &summaryWithAtime{summary{1, 4096}, 18, 0})
			So(dgut.store["/a/b/c/d"]["2\t10\t0"], ShouldBeNil)

			So(dgut.store["/a/b"][cuidKey], ShouldResemble, &summaryWithAtime{summary{3, 60}, 0, 0})
			So(dgut.store["/a/b"]["2\t2\t13"], ShouldResemble, &summaryWithAtime{summary{1, 5}, 0, 0})
			So(dgut.store["/a/b"]["2\t2\t6"], ShouldResemble, &summaryWithAtime{summary{1, 3}, 100, 0})
			So(dgut.store["/a/b"]["3\t2\t13"], ShouldResemble, &summaryWithAtime{summary{1, 6}, 0, 0})

			So(dgut.store["/a"][cuidKey], ShouldResemble, &summaryWithAtime{summary{3, 60}, 0, 0})
			So(dgut.store["/a"]["2\t2\t13"], ShouldResemble, &summaryWithAtime{summary{1, 5}, 0, 0})
			So(dgut.store["/a"]["2\t2\t6"], ShouldResemble, &summaryWithAtime{summary{1, 3}, 100, 0})
			So(dgut.store["/a"]["3\t2\t13"], ShouldResemble, &summaryWithAtime{summary{1, 6}, 0, 0})

			So(dgut.store["/"][cuidKey], ShouldResemble, &summaryWithAtime{summary{3, 60}, 0, 0})
			So(dgut.store["/"]["2\t2\t13"], ShouldResemble, &summaryWithAtime{summary{1, 5}, 0, 0})
			So(dgut.store["/"]["2\t2\t6"], ShouldResemble, &summaryWithAtime{summary{1, 3}, 100, 0})
			So(dgut.store["/"]["3\t2\t13"], ShouldResemble, &summaryWithAtime{summary{1, 6}, 0, 0})

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

					So(output, ShouldContainSubstring, "/a/b/c/d\t2\t10\t7\t1\t2\t200\t200\n")
					So(output, ShouldContainSubstring, "/a/b/c\t"+cuidKey+"\t2\t30\t0\t0\n")
					So(output, ShouldContainSubstring, "/a/b\t"+cuidKey+"\t3\t60\t0\t0\n")
					So(output, ShouldContainSubstring, "/a/b\t2\t2\t13\t1\t5\t0\t0\n")
					So(output, ShouldContainSubstring, "/a/b\t2\t2\t6\t1\t3\t100\t0\n")
					So(output, ShouldContainSubstring, "/\t3\t2\t13\t1\t6\t0\t0\n")

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
