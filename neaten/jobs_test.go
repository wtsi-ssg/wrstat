/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Author: Michael Woolnough <mw31@sanger.ac.uk>
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

package neaten

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/VertebrateResequencing/wr/client"
	"github.com/VertebrateResequencing/wr/jobqueue"
	. "github.com/smartystreets/goconvey/convey"
)

func TestRemoveOrLogJobs(t *testing.T) {
	Convey("You can log existing wrstat jobs and remove them", t, func() {
		client.PretendSubmissions = " "

		s, err := client.New(client.SchedulerSettings{})
		So(err, ShouldBeNil)

		r := client.DefaultRequirements()

		const (
			mountA = "/some/mount/point"
			mountB = "/some-other/mount/point"
		)

		jobs := []*jobqueue.Job{
			s.NewJob("wrstat multi", "bad-format", "", "", "", r),
			s.NewJob("wrstat walk "+mountA, "wrstat-walk-"+mountA+"-20200102-151413-AAA", "", "", "", r),
			s.NewJob("wrstat stat "+mountA, "wrstat-stat-"+mountA+"-20200102-151413-AAA", "", "", "", r),
			s.NewJob("wrstat walk "+mountB, "wrstat-walk-"+mountB+"-20200102-151413-AAA", "", "", "", r),
			s.NewJob("wrstat stat "+mountB, "wrstat-stat-"+mountB+"-20200102-151413-AAA", "", "", "", r),
		}

		So(s.SubmitJobs(jobs), ShouldBeNil)

		So(len(s.SubmittedJobs()), ShouldEqual, 5)

		tmp := t.TempDir()

		err = RemoveOrLogJobs(s, "AAA", tmp, true)
		So(err, ShouldBeNil)

		So(len(s.SubmittedJobs()), ShouldEqual, 1)

		f, err := os.Open(filepath.Join(tmp, "20200102151413-"+EncodePath(mountA), jobsFile))
		So(err, ShouldBeNil)

		var loggedJobs []*jobqueue.Job

		So(json.NewDecoder(f).Decode(&loggedJobs), ShouldBeNil)
		So(f.Close(), ShouldBeNil)
		So(loggedJobs, ShouldResemble, jobs[1:3])

		f, err = os.Open(filepath.Join(tmp, "20200102151413-"+EncodePath(mountB), jobsFile))
		So(err, ShouldBeNil)

		So(json.NewDecoder(f).Decode(&loggedJobs), ShouldBeNil)
		So(f.Close(), ShouldBeNil)
		So(loggedJobs, ShouldResemble, jobs[3:])
	})
}
