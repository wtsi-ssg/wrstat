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

	. "github.com/smartystreets/goconvey/convey"
)

func TestDirGUTFileType(t *testing.T) {
	Convey("DGUTFileType* consts are ints that can be stringified", t, func() {
		So(DirGUTFileType(0).String(), ShouldEqual, "cram")
		So(DGUTFileTypeCram.String(), ShouldEqual, "cram")
		So(DGUTFileTypeBam.String(), ShouldEqual, "bam")
		So(DGUTFileTypeIndex.String(), ShouldEqual, "index")
		So(DGUTFileTypeCompressed.String(), ShouldEqual, "compressed")
		So(DGUTFileTypeUncompressed.String(), ShouldEqual, "uncompressed")
		So(DGUTFileTypeCheckpoint.String(), ShouldEqual, "checkpoint")
		So(DGUTFileTypeOther.String(), ShouldEqual, "other")
		So(DGUTFileTypeTemp.String(), ShouldEqual, "temporary")
		So(int(DGUTFileTypeTemp), ShouldEqual, 7)
	})

	Convey("You can go from a string to a DGUTFileType", t, func() {
		ft, err := FileTypeStringToDirGUTFileType("cram")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypeCram)

		ft, err = FileTypeStringToDirGUTFileType("bam")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypeBam)

		ft, err = FileTypeStringToDirGUTFileType("index")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypeIndex)

		ft, err = FileTypeStringToDirGUTFileType("compressed")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypeCompressed)

		ft, err = FileTypeStringToDirGUTFileType("uncompressed")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypeUncompressed)

		ft, err = FileTypeStringToDirGUTFileType("checkpoint")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypeCheckpoint)

		ft, err = FileTypeStringToDirGUTFileType("other")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypeOther)

		ft, err = FileTypeStringToDirGUTFileType("temporary")
		So(err, ShouldBeNil)
		So(ft, ShouldEqual, DGUTFileTypeTemp)

		ft, err = FileTypeStringToDirGUTFileType("foo")
		So(err, ShouldNotBeNil)
		So(err, ShouldEqual, ErrInvalidType)
		So(ft, ShouldEqual, DGUTFileTypeOther)
	})

	Convey("isCram lets you know if a path is a cram file", t, func() {
		So(isCram("/foo/bar.cram"), ShouldBeTrue)
		So(isCram("/foo/bar.CRAM"), ShouldBeTrue)
		So(isCram("/foo/bar.bam"), ShouldBeFalse)
	})

	Convey("isBam lets you know if a path is a bam file", t, func() {
		So(isBam("/foo/bar.cram"), ShouldBeFalse)
		So(isBam("/foo/bar.bam"), ShouldBeTrue)
		So(isBam("/foo/bar.BAM"), ShouldBeTrue)
	})

	Convey("isIndex lets you know if a path is an index file", t, func() {
		So(isIndex("/foo/bar.cram"), ShouldBeFalse)
		So(isIndex("/foo/bar.cram.crai"), ShouldBeTrue)
		So(isIndex("/foo/bar.bai"), ShouldBeTrue)
		So(isIndex("/foo/bar.sai"), ShouldBeTrue)
		So(isIndex("/foo/bar.fai"), ShouldBeTrue)
		So(isIndex("/foo/bar.csi"), ShouldBeTrue)
		So(isIndex("/foo/bar.cSi"), ShouldBeTrue)
		So(isIndex("/foo/bar.yai"), ShouldBeFalse)
	})

	Convey("isCompressed lets you know if a path is a compressed file", t, func() {
		So(isCompressed("/foo/bar.bzip2"), ShouldBeTrue)
		So(isCompressed("/foo/bar.gz"), ShouldBeTrue)
		So(isCompressed("/foo/bar.tgz"), ShouldBeTrue)
		So(isCompressed("/foo/bar.zip"), ShouldBeTrue)
		So(isCompressed("/foo/bar.xz"), ShouldBeTrue)
		So(isCompressed("/foo/bar.bgz"), ShouldBeTrue)
		So(isCompressed("/foo/bar.bcf"), ShouldBeTrue)
		So(isCompressed("/foo/bar.bcF"), ShouldBeTrue)
		So(isCompressed("/foo/bar.asd"), ShouldBeFalse)
	})

	Convey("isUncompressed lets you know if a path is an uncompressed file", t, func() {
		So(isUncompressed("/foo/bar.sam"), ShouldBeTrue)
		So(isUncompressed("/foo/bar.fasta"), ShouldBeTrue)
		So(isUncompressed("/foo/bar.fastq"), ShouldBeTrue)
		So(isUncompressed("/foo/bar.fa"), ShouldBeTrue)
		So(isUncompressed("/foo/bar.fq"), ShouldBeTrue)
		So(isUncompressed("/foo/bar.vcf"), ShouldBeTrue)
		So(isUncompressed("/foo/bar.csv"), ShouldBeTrue)
		So(isUncompressed("/foo/bar.tsv"), ShouldBeTrue)
		So(isUncompressed("/foo/bar.txt"), ShouldBeTrue)
		So(isUncompressed("/foo/bar.text"), ShouldBeTrue)
		So(isUncompressed("/foo/bar.README"), ShouldBeTrue)
		So(isUncompressed("/foo/bar.o"), ShouldBeTrue)
		So(isUncompressed("/foo/bar.e"), ShouldBeTrue)
		So(isUncompressed("/foo/bar.oe"), ShouldBeTrue)
		So(isUncompressed("/foo/bar.dat"), ShouldBeTrue)
		So(isUncompressed("/foo/bar.Dat"), ShouldBeTrue)
		So(isUncompressed("/foo/bar.asd"), ShouldBeFalse)
	})

	Convey("isCheckpoint lets you know if a path is a checkpoint file", t, func() {
		So(isCheckpoint("/foo/bar.cram"), ShouldBeFalse)
		So(isCheckpoint("/foo/bar.jobstate.context"), ShouldBeTrue)
		So(isCheckpoint("/foo/bar.jobstate.coNtext"), ShouldBeTrue)
	})

	Convey("isTemp lets you know if a path is a temporary file", t, func() {
		So(isTemp("/foo/.tmp.cram"), ShouldBeTrue)
		So(isTemp("/foo/tmp.cram"), ShouldBeFalse)
		So(isTemp("/foo/tmp/bar.cram"), ShouldBeTrue)
		So(isTemp("/foo/a.cram.temp"), ShouldBeTrue)
		So(isTemp("/foo/attempt.cram"), ShouldBeFalse)
		So(isTemp("/foo/temp/bar.cram"), ShouldBeTrue)
		So(isTemp("/foo/TEMP/bar.cram"), ShouldBeTrue)
		So(isTemp("/foo/bar.cram"), ShouldBeFalse)
	})

	Convey("DirGroupUserType.pathToTypes lets you know the filetypes of a path", t, func() {
		d := NewByDirGroupUserType()

		So(d.pathToTypes("/foo/bar.cram"), ShouldResemble, []DirGUTFileType{DGUTFileTypeCram})
		So(pathToTypesMap(d, "/foo/.tmp.cram"), ShouldResemble, map[DirGUTFileType]bool{
			DGUTFileTypeCram: true, DGUTFileTypeTemp: true})
		So(d.pathToTypes("/foo/bar.bam"), ShouldResemble, []DirGUTFileType{DGUTFileTypeBam})
		So(d.pathToTypes("/foo/bar.crai"), ShouldResemble, []DirGUTFileType{DGUTFileTypeIndex})
		So(d.pathToTypes("/foo/bar.bzip2"), ShouldResemble, []DirGUTFileType{DGUTFileTypeCompressed})
		So(d.pathToTypes("/foo/bar.sam"), ShouldResemble, []DirGUTFileType{DGUTFileTypeUncompressed})
		So(d.pathToTypes("/foo/bar.jobstate.context"), ShouldResemble, []DirGUTFileType{DGUTFileTypeCheckpoint})
		So(d.pathToTypes("/foo/bar.sdf"), ShouldResemble, []DirGUTFileType{DGUTFileTypeOther})
		So(pathToTypesMap(d, "/foo/.tmp.sdf"), ShouldResemble, map[DirGUTFileType]bool{
			DGUTFileTypeOther: true, DGUTFileTypeTemp: true})
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
			err = dgut.Add("/a/b/c/3.bam", newMockInfo(2, 2, 3, false))
			So(err, ShouldBeNil)
			err = dgut.Add("/a/b/c/7.cram", newMockInfo(10, 2, 2, false))
			So(err, ShouldBeNil)
			err = dgut.Add("/a/b/c/8.cram", newMockInfo(2, 10, 2, false))
			So(err, ShouldBeNil)

			So(dgut.store["/a/b/c"], ShouldNotBeNil)
			So(dgut.store["/a/b"], ShouldNotBeNil)
			So(dgut.store["/a"], ShouldNotBeNil)
			So(dgut.store["/"], ShouldNotBeNil)
			So(dgut.store[""], ShouldBeNil)

			cuidKey := fmt.Sprintf("2\t%d\t4", cuid)
			So(dgut.store["/a/b/c"][cuidKey], ShouldResemble, &summary{2, 30})
			So(dgut.store["/a/b/c"]["2\t2\t4"], ShouldResemble, &summary{1, 5})
			So(dgut.store["/a/b/c"]["2\t2\t1"], ShouldResemble, &summary{1, 3})
			So(dgut.store["/a/b/c"]["3\t2\t4"], ShouldResemble, &summary{1, 6})
			So(dgut.store["/a/b/c"]["3\t2\t7"], ShouldBeNil)
			So(dgut.store["/a/b/c"]["2\t10\t0"], ShouldResemble, &summary{1, 2})
			So(dgut.store["/a/b/c"]["10\t2\t0"], ShouldResemble, &summary{1, 2})

			So(dgut.store["/a/b"][cuidKey], ShouldResemble, &summary{3, 60})
			So(dgut.store["/a/b"]["2\t2\t4"], ShouldResemble, &summary{1, 5})
			So(dgut.store["/a/b"]["2\t2\t1"], ShouldResemble, &summary{1, 3})
			So(dgut.store["/a/b"]["3\t2\t4"], ShouldResemble, &summary{1, 6})

			So(dgut.store["/a"][cuidKey], ShouldResemble, &summary{3, 60})
			So(dgut.store["/a"]["2\t2\t4"], ShouldResemble, &summary{1, 5})
			So(dgut.store["/a"]["2\t2\t1"], ShouldResemble, &summary{1, 3})
			So(dgut.store["/a"]["3\t2\t4"], ShouldResemble, &summary{1, 6})

			So(dgut.store["/"][cuidKey], ShouldResemble, &summary{3, 60})
			So(dgut.store["/"]["2\t2\t4"], ShouldResemble, &summary{1, 5})
			So(dgut.store["/"]["2\t2\t1"], ShouldResemble, &summary{1, 3})
			So(dgut.store["/"]["3\t2\t4"], ShouldResemble, &summary{1, 6})

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

					So(output, ShouldContainSubstring, "/a/b/c\t"+cuidKey+"\t2\t30\n")
					So(output, ShouldContainSubstring, "/a/b\t"+cuidKey+"\t3\t60\n")
					So(output, ShouldContainSubstring, "/a/b\t2\t2\t4\t1\t5\n")
					So(output, ShouldContainSubstring, "/a/b\t2\t2\t1\t1\t3\n")
					So(output, ShouldContainSubstring, "/\t3\t2\t4\t1\t6\n")

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
