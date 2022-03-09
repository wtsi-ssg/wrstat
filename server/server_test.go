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

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-resty/resty/v2"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wr/network/port"
	"github.com/wtsi-ssg/wrstat/dgut"
	"golang.org/x/sync/errgroup"
)

func TestServer(t *testing.T) {
	username, uid, gids := getUserAndGroups(t)

	Convey("Given a Server", t, func() {
		s := New()

		Convey("You can Start the Server", func() {
			checker, err := port.NewChecker("localhost")
			So(err, ShouldBeNil)
			port, _, err := checker.AvailableRange(2)
			So(err, ShouldBeNil)

			addr := fmt.Sprintf("localhost:%d", port)

			var g errgroup.Group
			g.Go(func() error {
				return s.Start(addr)
			})

			<-time.After(100 * time.Millisecond)
			client := resty.New()
			resp, err := client.R().Get("http://" + addr + "/foo")
			So(err, ShouldBeNil)
			So(resp.StatusCode(), ShouldEqual, http.StatusNotFound)

			s.Stop()
			err = g.Wait()
			So(err, ShouldBeNil)
		})

		if len(gids) < 2 {
			SkipConvey("Can't test the where endpoint without you belonging to at least 2 groups", func() {})

			return
		}

		Convey("convertSplitsValue works", func() {
			n := convertSplitsValue("1")
			So(n, ShouldEqual, 1)

			n = convertSplitsValue("foo")
			So(n, ShouldEqual, 2)
		})

		Convey("You can query the where endpoint", func() {
			response, err := queryWhere(s, "")
			So(err, ShouldBeNil)
			So(response.Code, ShouldEqual, http.StatusNotFound)

			Convey("And given a dgut database", func() {
				path, err := createExampleDB(t, uid, gids[0], gids[1])
				So(err, ShouldBeNil)

				tree, err := dgut.NewTree(path)
				So(err, ShouldBeNil)

				expected, err := tree.Where("/", nil, 2)
				So(err, ShouldBeNil)

				Convey("You can get results after calling LoadDGUTDB", func() {
					err = s.LoadDGUTDB(path)
					So(err, ShouldBeNil)

					response, err := queryWhere(s, "")
					So(err, ShouldBeNil)
					So(response.Code, ShouldEqual, http.StatusOK)

					result, err := decodeWhereResult(response)
					So(err, ShouldBeNil)
					So(result, ShouldResemble, expected)

					Convey("And you can filter results", func() {
						groups := gidsToGroups(t, gids...)

						matrix := map[string]dgut.DCSs{
							"?groups=" + groups[0] + "," + groups[1]: expected,
							"?groups=" + groups[0]: {
								{Dir: "/a/b", Count: 9, Size: 80},
								{Dir: "/a/b/d", Count: 7, Size: 70},
								{Dir: "/a/b/d/g", Count: 6, Size: 60},
								{Dir: "/a/b/e/h", Count: 2, Size: 10},
								{Dir: "/a/b/d/f", Count: 1, Size: 10},
								{Dir: "/a/b/e/h/tmp", Count: 1, Size: 5},
							},
							"?users=root," + username: expected,
							"?users=root": {
								{Dir: "/a", Count: 9, Size: 45},
								{Dir: "/a/b/d/g", Count: 4, Size: 40},
								{Dir: "/a/c/d", Count: 5, Size: 5},
							},
							"?groups=" + groups[0] + "&users=root": {
								{Dir: "/a/b/d/g", Count: 4, Size: 40},
							},
							"?types=cram,bam": expected,
							"?types=bam": {
								{Dir: "/a/b/e/h", Count: 2, Size: 10},
								{Dir: "/a/b/e/h/tmp", Count: 1, Size: 5},
							},
							"?groups=" + groups[0] + "&users=root&types=cram,bam": {
								{Dir: "/a/b/d/g", Count: 4, Size: 40},
							},
							"?groups=" + groups[0] + "&users=root&types=bam": {
								{Dir: "/", Count: 0, Size: 0},
							},
							"?splits=0": {
								{Dir: "/a", Count: 14, Size: 85},
							},
							"?dir=/a/b/e/h": {
								{Dir: "/a/b/e/h", Count: 2, Size: 10},
								{Dir: "/a/b/e/h/tmp", Count: 1, Size: 5},
							},
						}

						runMapMatrixTest(t, matrix, s)
					})

					Convey("Where bad filters fail", func() {
						badFilters := []string{
							"?groups=fo#€o",
							"?users=fo#€o",
							"?types=fo#€o",
						}

						runSliceMatrixTest(t, badFilters, s)
					})

					Convey("Unless you provide an invalid directory", func() {
						response, err := queryWhere(s, "?dir=/foo")
						So(err, ShouldBeNil)
						So(response.Code, ShouldEqual, http.StatusBadRequest)
					})
				})
			})

			Convey("LoadDGUTDB fails on an invalid path", func() {
				err := s.LoadDGUTDB("/foo")
				So(err, ShouldNotBeNil)
			})
		})
	})
}

// getUserAndGroups returns the current users username, uid and gids.
func getUserAndGroups(t *testing.T) (string, string, []string) {
	t.Helper()

	uu, err := user.Current()
	if err != nil {
		t.Logf("getting current user failed: %s", err.Error())

		return "", "", nil
	}

	gids, err := uu.GroupIds()
	if err != nil {
		t.Logf("getting group ids failed: %s", err.Error())

		return "", "", nil
	}

	return uu.Username, uu.Uid, gids
}

// queryWhere does a test GET of /where, with extra appended (start it with ?).
func queryWhere(s *Server, extra string) (*httptest.ResponseRecorder, error) {
	response := httptest.NewRecorder()

	req, err := http.NewRequestWithContext(context.Background(), "GET", "/where"+extra, nil)
	if err != nil {
		return nil, err
	}

	s.router.ServeHTTP(response, req)

	return response, nil
}

// decodeWhereResult decodes the result of a Where query.
func decodeWhereResult(response *httptest.ResponseRecorder) (dgut.DCSs, error) {
	var result dgut.DCSs
	err := json.NewDecoder(response.Body).Decode(&result)

	return result, err
}

// createExampleDB creates a temporary dgut.db from some example data that uses
// the given uid and gids, and returns the path to the database file.
func createExampleDB(t *testing.T, uid, gidA, gidB string) (string, error) {
	t.Helper()

	dir := t.TempDir()
	path := filepath.Join(dir, "dgut.db")

	dgutData := exampleDGUTData(uid, gidA, gidB)
	data := strings.NewReader(dgutData)
	db := dgut.NewDB(path)

	err := db.Store(data, 20)

	return path, err
}

// exampleDGUTData is some example DGUT data that uses the given uid and gids,
// along with root's uid.
func exampleDGUTData(uid, gidA, gidB string) string {
	data := `/	x	z	0	3	30
/	x	z	1	2	10
/	x	z	7	1	5
/	x	0	0	4	40
/	y	0	0	5	5
/a	x	z	0	3	30
/a	x	z	1	2	10
/a	x	z	7	1	5
/a	x	0	0	4	40
/a	y	0	0	5	5
/a/b	x	z	0	3	30
/a/b	x	z	1	2	10
/a/b	x	z	7	1	5
/a/b	x	0	0	4	40
/a/b/d	x	z	0	3	30
/a/b/d	x	0	0	4	40
/a/b/d/f	x	z	0	1	10
/a/b/d/g	x	z	0	2	20
/a/b/d/g	x	0	0	4	40
/a/b/e	x	z	1	2	10
/a/b/e	x	z	7	1	5
/a/b/e/h	x	z	1	2	10
/a/b/e/h	x	z	7	1	5
/a/b/e/h/tmp	x	z	1	1	5
/a/b/e/h/tmp	x	z	7	1	5
/a/c	y	0	0	5	5
/a/c/d	y	0	0	5	5
`

	data = strings.ReplaceAll(data, "x", gidA)
	data = strings.ReplaceAll(data, "y", gidB)
	data = strings.ReplaceAll(data, "z", uid)

	return data
}

// gidsToGroups converts the given gids to group names.
func gidsToGroups(t *testing.T, gids ...string) []string {
	t.Helper()

	groups := make([]string, len(gids))

	for i, gid := range gids {
		g, err := user.LookupGroupId(gid)
		if err != nil {
			t.Fatalf("LookupGroupId(%s) failed: %s", gid, err)
		}

		groups[i] = g.Name
	}

	return groups
}

// runMapMatrixTest tests queries against expected results on the Server.
func runMapMatrixTest(t *testing.T, matrix map[string]dgut.DCSs, s *Server) {
	t.Helper()

	for filter, exp := range matrix {
		response, err := queryWhere(s, filter)
		So(err, ShouldBeNil)
		So(response.Code, ShouldEqual, http.StatusOK)

		result, err := decodeWhereResult(response)
		So(err, ShouldBeNil)
		So(result, ShouldResemble, exp)
	}
}

// runSliceMatrixTest tests queries that are expected to fail on the Server.
func runSliceMatrixTest(t *testing.T, matrix []string, s *Server) {
	t.Helper()

	for _, filter := range matrix {
		response, err := queryWhere(s, filter)
		So(err, ShouldBeNil)
		So(response.Code, ShouldEqual, http.StatusBadRequest)
	}
}
