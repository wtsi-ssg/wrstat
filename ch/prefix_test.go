/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
 *          Sendu Bala <sb10@sanger.ac.uk>
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

package ch

import (
	"path/filepath"
	"strconv"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

const prefixBenchmarkNumPrefixes = 1000

func TestPrefixTree(t *testing.T) {
	prefixBenchmarkPrefixes := makePrefixBenchmarkPrefixes()

	Convey("pathPrefixTree longestPrefix works", t, func() {
		tree := newPathPrefixTree()

		for _, prefix := range prefixBenchmarkPrefixes {
			tree.addDirectory(prefix)
		}

		_, found := tree.longestPrefix("/9/file.txt")
		So(found, ShouldBeFalse)

		_, found = tree.longestPrefix("/0/file.txt")
		So(found, ShouldBeFalse)

		prefix, found := tree.longestPrefix(filepath.Join(prefixBenchmarkPrefixes[0], "sub", "file.txt"))
		So(found, ShouldBeTrue)
		So(prefix, ShouldEqual, prefixBenchmarkPrefixes[0])

		prefix, found = tree.longestPrefix(
			filepath.Join(prefixBenchmarkPrefixes[prefixBenchmarkNumPrefixes-1], "file.txt"))
		So(found, ShouldBeTrue)
		So(prefix, ShouldEqual, prefixBenchmarkPrefixes[prefixBenchmarkNumPrefixes-1])
	})
}

func makePrefixBenchmarkPrefixes() []string {
	prefixBenchmarkPrefixes := make([]string, 0, prefixBenchmarkNumPrefixes)

	cb := func(path string) {
		prefixBenchmarkPrefixes = append(prefixBenchmarkPrefixes, path)
	}

	makePrefixPath("/", 5, 8, cb)
	makePrefixPath("/", 5, 6, cb)
	makePrefixPath("/", 5, 5, cb)
	makePrefixPath("/", 5, 4, cb)
	makePrefixPath("/", 5, 3, cb)

	return prefixBenchmarkPrefixes
}

func makePrefixPath(path string, max, remaining int, cb func(string)) {
	if remaining == 0 {
		cb(path)

		return
	}

	for i := 0; i < max; i++ {
		iDir := strconv.Itoa(i)

		makePrefixPath(filepath.Join(path, iDir), max, remaining-1, cb)
	}
}
