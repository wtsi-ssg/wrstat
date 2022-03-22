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

// package server provides a web server for a REST API and website.

package server

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os/user"
	"strconv"
	"strings"
	"time"

	"github.com/gin-contrib/secure"
	"github.com/gin-gonic/gin"
	"github.com/wtsi-ssg/wrstat/dgut"
	"github.com/wtsi-ssg/wrstat/summary"
	"gopkg.in/tylerb/graceful.v1"
)

const (
	// EndPointREST is the base location for all REST endpoints.
	EndPointREST = "/rest/v1"

	// EndPointJWT is the endpoint for creating or refreshing a JWT.
	EndPointJWT = EndPointREST + "/jwt"

	// EndPointAuth is the name of the router group that endpoints requiring JWT
	// authorisation should belong to.
	EndPointAuth = EndPointREST + "/auth"

	wherePath = "/where"

	// EndPointWhere is the endpoint for making where queries if authorization
	// isn't implemented.
	EndPointWhere = EndPointREST + wherePath

	// EndPointAuthWhere is the endpoint for making where queries if
	// authorization is implemented.
	EndPointAuthWhere = EndPointAuth + wherePath

	defaultDir    = "/"
	defaultSplits = "2"
	stopTimeout   = 10 * time.Second
)

// AuthCallback is a function that returns true if the given password is valid
// for the given username. It also returns the other UIDs this user can sudo as,
// and all the groups this user and the sudoable users belong to.
//
// As a special case, if the user can sudo as root, it should just return
// nil slices.
type AuthCallback func(username, password string) (bool, []string, []string)

// Server is used to start a web server that provides a REST API to the dgut
// package's database, and a website that displays the information nicely.
type Server struct {
	router    *gin.Engine
	tree      *dgut.Tree
	srv       *graceful.Server
	authGroup *gin.RouterGroup
	authCB    AuthCallback
}

// New creates a Server which can serve a REST API and website.
//
// It logs to the given io.Writer, which could for example be syslog using the
// log/syslog pkg with syslog.new(syslog.LOG_INFO, "tag").
func New(logWriter io.Writer) *Server {
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()

	logger := log.New(logWriter, "", 0)

	gin.DisableConsoleColor()
	gin.DefaultWriter = logger.Writer()

	r.Use(gin.LoggerWithFormatter(func(param gin.LogFormatterParams) string {
		return fmt.Sprintf("%s - [%s %s %s \"%s\"] STATUS=%d %s %s\n",
			param.ClientIP,
			param.Method,
			param.Path,
			param.Request.Proto,
			param.Request.UserAgent(),
			param.StatusCode,
			param.Latency,
			param.ErrorMessage,
		)
	}))

	r.Use(gin.RecoveryWithWriter(logWriter))

	return &Server{
		router: r,
	}
}

// Start will start listening to the given address (eg. "localhost:8080"), and
// serve the REST API and website over https; you must provide paths to your
// certficate and key file.
//
// It blocks, but will gracefully shut down on SIGINT and SIGTERM. If you
// Start() in a go-routine, you can call Stop() manually.
func (s *Server) Start(addr, certFile, keyFile string) error {
	s.router.Use(secure.New(secure.DefaultConfig()))

	srv := &graceful.Server{
		Timeout: stopTimeout,

		Server: &http.Server{
			Addr:    addr,
			Handler: s.router,
		},
	}

	s.srv = srv

	return srv.ListenAndServeTLS(certFile, keyFile)
}

// Stop() gracefully stops the server after Start(), and waits for active
// connections to close and the port to be available again.
func (s *Server) Stop() {
	ch := s.srv.StopChan()
	s.srv.Stop(stopTimeout)
	<-ch
}

// LoadDGUTDBs loads the given dgut.db files (as produced by one or more
// invocations of dgut.DB.Store()) and adds the /rest/v1/where GET endpoint to
// the REST API. If you call EnableAuth() first, then this endpoint will be
// secured and be available at /rest/v1/auth/where.
//
// The where endpoint can take the dir, splits, groups, users and types
// parameters, which correspond to arguments that dgut.Tree.Where() takes.
func (s *Server) LoadDGUTDBs(paths ...string) error {
	tree, err := dgut.NewTree(paths...)
	if err != nil {
		return err
	}

	s.tree = tree

	if s.authGroup == nil {
		s.router.GET(EndPointWhere, s.getWhere)
	} else {
		s.authGroup.GET(wherePath, s.getWhere)
	}

	return nil
}

// getWhere responds with a list of directory stats describing where data is on
// disks. LoadDGUTDB() must already have been called. This is called when there
// is a GET on /rest/v1/where.
func (s *Server) getWhere(c *gin.Context) {
	dir := c.DefaultQuery("dir", defaultDir)
	splits := c.DefaultQuery("splits", defaultSplits)
	groups := c.Query("groups")
	users := c.Query("users")
	types := c.Query("types")

	filterGIDs, err := s.restrictedGroups(c, groups)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	filterUIDs, err := s.restrictedUsers(c, users)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	dcss, err := s.callWhere(dir, splits, filterGIDs, filterUIDs, types)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	c.IndentedJSON(http.StatusOK, dcss)
}

// restrictedGroups checks our JWT if present, and will return the GIDs that
// user is allowed to query. If groups arg is not blank, but a comma separated
// list of group names, further limits the GIDs returned to be amongst those. If
// the JWT has no groups specified, returns all the given group names as GIDs.
func (s *Server) restrictedGroups(c *gin.Context, groups string) ([]string, error) {
	ids, wanted, err := getWantedIDs(groups, groupNameToGID)
	if err != nil {
		return nil, err
	}

	allowedIDs := s.getRestrictedIDs(c, func(u *User) []string {
		return u.GIDs
	})

	if allowedIDs == nil {
		return ids, nil
	}

	return restrictIDsToWanted(allowedIDs, wanted)
}

// groupNameToGID converts group name to GID.
func groupNameToGID(name string) (string, error) {
	g, err := user.LookupGroup(name)
	if err != nil {
		return "", err
	}

	return g.Gid, nil
}

// getWantedIDs splits the given comma separated names in to a slice and then
// passes each name to the given callback to convert it to an id, then returns
// a slice of the ids, along with a map where the slice elements are the keys.
// Both will be nil if names is blank.
func getWantedIDs(names string, cb func(name string) (string, error)) ([]string, map[string]bool, error) {
	splitNames := splitCommaSeparatedString(names)

	ids := make([]string, len(splitNames))
	wanted := make(map[string]bool, len(splitNames))

	for i, name := range splitNames {
		id, err := cb(name)
		if err != nil {
			return nil, nil, err
		}

		ids[i] = id
		wanted[id] = true
	}

	return ids, wanted, nil
}

// splitCommaSeparatedString splits the given comma separated string in to a
// slice of string. Returns nil if value is blank.
func splitCommaSeparatedString(value string) []string {
	var parts []string
	if value != "" {
		parts = strings.Split(value, ",")
	}

	return parts
}

// getRestrictedIDs extracts the User information from our JWT and passes it to
// the given callback, which should return the desired type of ID (GIDs or
// UIDs). Returns nil without calling the callback if we're not doing auth.
func (s *Server) getRestrictedIDs(c *gin.Context, cb func(*User) []string) []string {
	if s.authGroup == nil {
		return nil
	}

	u := s.getUser(c)

	return cb(u)
}

// restrictIDsToWanted returns the elements of ids that are in wanted. Will
// return ids if wanted is empty. Returns an error if you don't want any of the
// given ids.
func restrictIDsToWanted(ids []string, wanted map[string]bool) ([]string, error) {
	if len(wanted) == 0 {
		return ids, nil
	}

	var final []string //nolint:prealloc

	for _, id := range ids {
		if !wanted[id] {
			continue
		}

		final = append(final, id)
	}

	if final == nil {
		return nil, ErrBadQuery
	}

	return final, nil
}

// restrictedUsers checks our JWT if present, and will return the user IDs that
// user is allowed to query. If users arg is not blank, but a comma separated
// list of user names, further limits the users returned to be amongst those.
// If the JWT has no users specified, returns all the given users as UIDs.
func (s *Server) restrictedUsers(c *gin.Context, users string) ([]string, error) {
	ids, wanted, err := getWantedIDs(users, userNameToUID)
	if err != nil {
		return nil, err
	}

	allowedIDs := s.getRestrictedIDs(c, func(u *User) []string {
		return u.UIDs
	})

	if allowedIDs == nil {
		return ids, nil
	}

	return restrictIDsToWanted(allowedIDs, wanted)
}

// userNameToUID converts user name to UID.
func userNameToUID(name string) (string, error) {
	u, err := user.Lookup(name)
	if err != nil {
		return "", err
	}

	return u.Uid, nil
}

// callWhere interprets string filters and passes them to tree.Where().
func (s *Server) callWhere(dir, splits string, gids, uids []string, types string) (dgut.DCSs, error) {
	filter, err := makeTreeFilter(gids, uids, types)
	if err != nil {
		return nil, err
	}

	return s.tree.Where(dir, filter, convertSplitsValue(splits))
}

// makeTreeFilter creates a filter from string args.
func makeTreeFilter(gids, uids []string, types string) (*dgut.Filter, error) {
	filter := makeTreeGroupFilter(gids)

	addUsersToFilter(filter, uids)

	err := addTypesToFilter(filter, types)

	return filter, err
}

// makeTreeGroupFilter creates a filter for groups.
func makeTreeGroupFilter(gids []string) *dgut.Filter {
	if len(gids) == 0 {
		return &dgut.Filter{}
	}

	return &dgut.Filter{GIDs: idStringsToInts(gids)}
}

// idStringsToInts converts a slice of id strings into uint32s.
func idStringsToInts(idStrings []string) []uint32 {
	ids := make([]uint32, len(idStrings))

	for i, idStr := range idStrings {
		// no error is possible here, with the number string coming from an OS
		// lookup.
		//nolint:errcheck
		id, _ := strconv.ParseUint(idStr, 10, 32)

		ids[i] = uint32(id)
	}

	return ids
}

// addUsersToFilter adds a filter for users to the given filter.
func addUsersToFilter(filter *dgut.Filter, uids []string) {
	if len(uids) == 0 {
		return
	}

	filter.UIDs = idStringsToInts(uids)
}

// addTypesToFilter adds a filter for types to the given filter.
func addTypesToFilter(filter *dgut.Filter, types string) error {
	if types == "" {
		return nil
	}

	tnames := splitCommaSeparatedString(types)
	fts := make([]summary.DirGUTFileType, len(tnames))

	for i, name := range tnames {
		ft, err := summary.FileTypeStringToDirGUTFileType(name)
		if err != nil {
			return err
		}

		fts[i] = ft
	}

	filter.FTs = fts

	return nil
}

// convertSplitsValue converts the given number string in to an int. On failure,
// returns our default value for splits of 2.
func convertSplitsValue(splits string) int {
	splitsN, err := strconv.ParseUint(splits, 10, 8)
	if err != nil {
		return convertSplitsValue(defaultSplits)
	}

	return int(splitsN)
}
