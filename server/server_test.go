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
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
	"testing"
	"time"

	jwt "github.com/appleboy/gin-jwt/v2"
	"github.com/gin-gonic/gin"
	"github.com/go-resty/resty/v2"
	gjwt "github.com/golang-jwt/jwt/v4"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-ssg/wr/network/port"
	"github.com/wtsi-ssg/wrstat/dgut"
	"golang.org/x/sync/errgroup"
)

func TestServer(t *testing.T) {
	username, uid, gids := getUserAndGroups(t)

	exampleUser := &User{Username: "user", UIDs: []string{"1", "2"}, GIDs: []string{"3", "4"}}

	Convey("hasError tells you about errors", t, func() {
		So(hasError(nil, nil), ShouldBeFalse)
		So(hasError(nil, ErrBadQuery, nil), ShouldBeTrue)
	})

	Convey("Given a Server", t, func() {
		var logWriter strings.Builder
		s := New(&logWriter)

		Convey("You can Start the Server", func() {
			checker, err := port.NewChecker("localhost")
			So(err, ShouldBeNil)
			port, _, err := checker.AvailableRange(2)
			So(err, ShouldBeNil)

			addr := fmt.Sprintf("localhost:%d", port)
			certPath, keyPath, err := createTestCert(t)
			So(err, ShouldBeNil)

			var g errgroup.Group
			g.Go(func() error {
				return s.Start(addr, certPath, keyPath)
			})

			<-time.After(100 * time.Millisecond)

			defer func() {
				s.Stop()
				err = g.Wait()
				So(err, ShouldBeNil)
			}()

			client := resty.New()
			client.SetRootCertificate(certPath)

			resp, err := client.R().Get("http://" + addr + "/foo")
			So(err, ShouldBeNil)
			So(resp.StatusCode(), ShouldEqual, http.StatusBadRequest)

			resp, err = client.R().Get("https://" + addr + "/foo")
			So(err, ShouldBeNil)
			So(resp.StatusCode(), ShouldEqual, http.StatusNotFound)

			Convey("The jwt endpoint works after enabling it", func() {
				err = s.EnableAuth("/foo", "/bar", func(u, p string) (bool, []string, []string) {
					return false, nil, nil
				})
				So(err, ShouldNotBeNil)

				err = s.EnableAuth(certPath, keyPath, func(u, p string) (bool, []string, []string) {
					ok := p == "pass"

					return ok, []string{"1", "2"}, []string{"3", "4"}
				})
				So(err, ShouldBeNil)

				r := newClientRequest(addr, certPath)
				resp, err = r.Post(EndPointJWT)
				So(err, ShouldBeNil)
				So(resp.String(), ShouldEqual, `{"code":401,"message":"missing Username or Password"}`)

				_, err = Login("foo", certPath, "user", "foo")
				So(err, ShouldNotBeNil)

				var token string
				token, err = Login(addr, certPath, "user", "foo")
				So(err, ShouldNotBeNil)
				So(err, ShouldEqual, ErrNoAuth)
				So(token, ShouldBeBlank)

				token, err = Login(addr, certPath, "user", "pass")
				So(err, ShouldBeNil)
				So(token, ShouldNotBeBlank)

				var called int
				var claims jwt.MapClaims
				var userI interface{}
				var gu *User

				s.authGroup.GET("/test", func(c *gin.Context) {
					called++
					userI, _ = c.Get(userKey)
					gu = s.getUser(c)
					claims = jwt.ExtractClaims(c)
				})

				resp, err = r.Get(EndPointAuth + "/test")
				So(err, ShouldBeNil)
				So(resp.String(), ShouldEqual, `{"code":401,"message":"auth header is empty"}`)

				r = newAuthenticatedClientRequest(addr, certPath, "{sdf.sdf.sdf}")
				resp, err = r.Get(EndPointAuth + "/test")
				So(err, ShouldBeNil)
				So(resp.String(), ShouldEqual, `{"code":401,"message":"illegal base64 data at input byte 0"}`)

				start := time.Now()
				end := start.Add(1 * time.Minute)

				var noClaimToken string
				noClaimToken, err = makeTestToken(keyPath, start, end, false)
				So(err, ShouldBeNil)

				r = newAuthenticatedClientRequest(addr, certPath, noClaimToken)
				resp, err = r.Get(EndPointAuth + "/test")
				So(err, ShouldBeNil)
				So(resp.String(), ShouldEqual, `{"code":403,"message":"you don't have permission to access this resource"}`)

				var keyPath2 string
				_, keyPath2, err = createTestCert(t)
				So(err, ShouldBeNil)

				var manualWronglySignedToken string
				manualWronglySignedToken, err = makeTestToken(keyPath2, start, end, true)
				So(err, ShouldBeNil)

				r = newAuthenticatedClientRequest(addr, certPath, manualWronglySignedToken)
				resp, err = r.Get(EndPointAuth + "/test")
				So(err, ShouldBeNil)
				So(resp.String(), ShouldEqual, `{"code":401,"message":"crypto/rsa: verification error"}`)

				var manualCorrectlySignedToken string
				manualCorrectlySignedToken, err = makeTestToken(keyPath, start, end, true)
				So(err, ShouldBeNil)

				r = newAuthenticatedClientRequest(addr, certPath, manualCorrectlySignedToken)
				resp, err = r.Get(EndPointAuth + "/test")
				So(err, ShouldBeNil)
				So(resp.String(), ShouldBeBlank)

				var manualExpiredToken string
				manualExpiredToken, err = makeTestToken(keyPath, start, start.Add(time.Nanosecond), true)
				So(err, ShouldBeNil)

				r = newAuthenticatedClientRequest(addr, certPath, manualExpiredToken)
				resp, err = r.Get(EndPointAuth + "/test")
				So(err, ShouldBeNil)
				So(resp.String(), ShouldEqual, `{"code":401,"message":"Token is expired"}`)

				_, err = RefreshJWT("foo", certPath, manualExpiredToken)
				So(err, ShouldNotBeNil)

				_, err = RefreshJWT(addr, certPath, manualWronglySignedToken)
				So(err, ShouldNotBeNil)

				var refreshedToken string
				refreshedToken, err = RefreshJWT(addr, certPath, manualExpiredToken)
				So(err, ShouldBeNil)
				So(refreshedToken, ShouldNotBeBlank)

				r = newAuthenticatedClientRequest(addr, certPath, refreshedToken)
				resp, err = r.Get(EndPointAuth + "/test")
				So(err, ShouldBeNil)
				So(resp.String(), ShouldBeBlank)

				past := start.Add(-(2 * tokenDuration) - (2 * time.Nanosecond))
				manualExpiredToken, err = makeTestToken(keyPath, past, past.Add(time.Nanosecond), true)
				So(err, ShouldBeNil)

				_, err = RefreshJWT(addr, certPath, manualExpiredToken)
				So(err, ShouldNotBeNil)

				r = newAuthenticatedClientRequest(addr, certPath, token)
				resp, err = r.Get(EndPointAuth + "/test")
				So(err, ShouldBeNil)
				So(resp.String(), ShouldBeBlank)

				So(called, ShouldEqual, 3)
				So(claims[userKey], ShouldBeNil)
				So(claims[claimKeyUsername], ShouldEqual, "user")
				user, ok := userI.(*User)
				So(ok, ShouldBeTrue)
				So(user, ShouldResemble, exampleUser)
				So(gu, ShouldResemble, exampleUser)
			})

			Convey("authPayLoad correctly maps a User to claims, or returns none", func() {
				data := "foo"
				claims := authPayLoad(data)
				So(len(claims), ShouldEqual, 0)

				claims = authPayLoad(exampleUser)
				So(len(claims), ShouldEqual, 3)
				So(claims, ShouldResemble, jwt.MapClaims{
					claimKeyUsername: "user",
					claimKeyUIDs:     "1,2",
					claimKeyGIDs:     "3,4",
				})
			})

			Convey("retrieveClaimString fails with bad claims", func() {
				claims := jwt.MapClaims{"foo": []string{"bar"}}

				_, errc := retrieveClaimString(claims, "abc")
				So(errc, ShouldNotBeNil)

				str, errc := retrieveClaimString(claims, "foo")
				So(errc, ShouldNotBeNil)
				So(errc, ShouldEqual, ErrBadJWTClaim)
				So(str, ShouldBeBlank)
			})

			Convey("getUser fails without the user key having a valid value", func() {
				called := 0

				var user1, user2 *User

				s.router.GET("/test", func(c *gin.Context) {
					user1 = s.getUser(c)
					c.Keys = map[string]interface{}{userKey: "foo"}
					user2 = s.getUser(c)

					called++
				})

				r := newClientRequest(addr, certPath)
				resp, err = r.Get("https://" + addr + "/test")
				So(err, ShouldBeNil)

				So(called, ShouldEqual, 1)
				So(user1, ShouldBeNil)
				So(user2, ShouldBeNil)
			})

			testWhereClientOnRealServer(t, uid, gids, s, addr, certPath, keyPath)
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
			So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/where")
			So(logWriter.String(), ShouldContainSubstring, "STATUS=404")
			logWriter.Reset()

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
					So(logWriter.String(), ShouldContainSubstring, "[GET /rest/v1/where")
					So(logWriter.String(), ShouldContainSubstring, "STATUS=200")

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
						So(logWriter.String(), ShouldContainSubstring, "STATUS=400")
						So(logWriter.String(), ShouldContainSubstring, "Error #01: directory not found")
					})
				})
			})

			Convey("LoadDGUTDB fails on an invalid path", func() {
				err := s.LoadDGUTDB("/foo")
				So(err, ShouldNotBeNil)
			})
		})

		Convey("Endpoints that panic are logged", func() {
			s.router.GET("/foo", func(c *gin.Context) {
				panic("bar")
			})

			response, err := queryREST(s, "/foo", "")
			So(err, ShouldBeNil)
			So(response.Code, ShouldEqual, http.StatusInternalServerError)
			So(logWriter.String(), ShouldContainSubstring, "STATUS=500")
			So(logWriter.String(), ShouldContainSubstring, "panic")
		})
	})
}

// testWhereClientOnRealServer tests our client method GetWhereDataIs on a real
// listening server, if we have at least 2 gids to test with.
func testWhereClientOnRealServer(t *testing.T, uid string, gids []string, s *Server, addr, cert, key string) {
	t.Helper()

	if len(gids) < 2 {
		return
	}

	Convey("The where endpoint works with a real server", func() {
		_, _, err := GetWhereDataIs("localhost:1", cert, "", "", "", "", "", "")
		So(err, ShouldNotBeNil)

		path, err := createExampleDB(t, uid, gids[0], gids[1])
		So(err, ShouldBeNil)

		err = s.LoadDGUTDB(path)
		So(err, ShouldBeNil)

		_, _, err = GetWhereDataIs(addr, cert, "", "/", "", "", "", "")
		So(err, ShouldNotBeNil)
		So(err, ShouldEqual, ErrNoAuth)

		g, errg := user.LookupGroupId(gids[0])
		So(errg, ShouldBeNil)

		Convey("Root can see everything", func() {
			err = s.EnableAuth(cert, key, func(username, password string) (bool, []string, []string) {
				return true, nil, nil
			})
			So(err, ShouldBeNil)

			err = s.LoadDGUTDB(path)
			So(err, ShouldBeNil)

			token, errl := Login(addr, cert, "user", "pass")
			So(errl, ShouldBeNil)

			_, _, err = GetWhereDataIs(addr, cert, token, "", "", "", "", "")
			So(err, ShouldNotBeNil)
			So(err, ShouldEqual, ErrBadQuery)

			json, dcss, errg := GetWhereDataIs(addr, cert, token, "/", "", "", "", "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 14)

			json, dcss, errg = GetWhereDataIs(addr, cert, token, "/", g.Name, "", "", "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 9)
		})

		Convey("Normal users have restricted access", func() {
			err = s.EnableAuth(cert, key, func(username, password string) (bool, []string, []string) {
				return true, []string{uid}, gids
			})
			So(err, ShouldBeNil)

			err = s.LoadDGUTDB(path)
			So(err, ShouldBeNil)

			token, errl := Login(addr, cert, "user", "pass")
			So(errl, ShouldBeNil)

			json, dcss, errg := GetWhereDataIs(addr, cert, token, "/", "", "", "", "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 5)

			json, dcss, errg = GetWhereDataIs(addr, cert, token, "/", g.Name, "", "", "0")
			So(errg, ShouldBeNil)
			So(string(json), ShouldNotBeBlank)
			So(len(dcss), ShouldEqual, 1)
			So(dcss[0].Count, ShouldEqual, 5)
		})
	})
}

// createTestCert creates a self-signed cert and key, returning their paths.
func createTestCert(t *testing.T) (string, string, error) {
	t.Helper()

	dir := t.TempDir()
	certPath := filepath.Join(dir, "cert")
	keyPath := filepath.Join(dir, "key")

	cmd := exec.Command("openssl", "req", "-new", "-newkey", "rsa:4096",
		"-days", "1", "-nodes", "-x509", "-subj", "/CN=localhost",
		"-addext", "subjectAltName = DNS:localhost",
		"-keyout", keyPath, "-out", certPath,
	)

	err := cmd.Run()

	return certPath, keyPath, err
}

// makeTestToken creates a JWT signed with the key at the given path, that
// has orig_iat of start and exp of end, and includes a claimKeyUsername claim
// if withUserClaims is true.
func makeTestToken(keyPath string, start, end time.Time, withUserClaims bool) (string, error) {
	token := gjwt.New(gjwt.GetSigningMethod("RS512"))

	claims, ok := token.Claims.(gjwt.MapClaims)
	if !ok {
		return "", ErrNoAuth
	}

	if withUserClaims {
		claims[claimKeyUsername] = "root"
		claims[claimKeyUIDs] = ""
		claims[claimKeyGIDs] = ""
	}

	claims["orig_iat"] = start.Unix()
	claims["exp"] = end.Unix()

	keyData, err := ioutil.ReadFile(keyPath)
	if err != nil {
		return "", err
	}

	key, err := gjwt.ParseRSAPrivateKeyFromPEM(keyData)
	if err != nil {
		return "", err
	}

	return token.SignedString(key)
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

// queryWhere does a test GET of /rest/v1/where, with extra appended (start it
// with ?).
func queryWhere(s *Server, extra string) (*httptest.ResponseRecorder, error) {
	return queryREST(s, EndPointWhere, extra)
}

// queryREST does a test GET of the given REST endpoint (start it with /), with
// extra appended (start it with ?).
func queryREST(s *Server, endpoint, extra string) (*httptest.ResponseRecorder, error) {
	response := httptest.NewRecorder()

	req, err := http.NewRequestWithContext(context.Background(), "GET", endpoint+extra, nil)
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
