/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * 		   Kyle Mace  <km34@sanger.ac.uk>
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

package combine

import (
	"os"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wrstat/v3/fs"
)

func TestByUserGroupFiles(t *testing.T) {
	Convey("Given byusergroup files and an output", t, func() {
		inputs, output, outputPath := BuildCombineAndUserGroupInputs(t,
			[]string{"walk.1.byusergroup", "walk.2.byusergroup", "walk.3.byusergroup",
				"walk.4.byusergroup", "walk.5.byusergroup", "walk.6.byusergroup"},
			[]string{"david", "fred", "graham"},
			[]string{"adam", "ben", "charlie"},
			[]string{"short/test/dir/", "test/dir/that/is/much/longer"})

		expectedMergeOutput := "david\tadam\tshort/test/dir/\t3\t5\n" +
			"fred\tben\tshort/test/dir/\t3\t4\n" +
			"fred\tben\ttest/dir/that/is/much/longer\t4\t5\n" +
			"graham\tcharlie\ttest/dir/that/is/much/longer\t11\t13\n"

		Convey("You can merge and compress the byusergroup files to the output", func() {
			err := UserGroupFiles(inputs, output)
			So(err, ShouldBeNil)

			_, err = os.Stat(outputPath)
			So(err, ShouldBeNil)

			Convey("The proper content exists within the output file", func() {
				actualContent, err := fs.ReadCompressedFile(outputPath)
				So(err, ShouldBeNil)

				So(actualContent, ShouldEqual, expectedMergeOutput)
			})
		})
	})
}
