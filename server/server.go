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
	"embed"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-contrib/secure"
	"github.com/gin-gonic/gin"
	"github.com/wtsi-ssg/wrstat/dgut"
	"github.com/wtsi-ssg/wrstat/summary"
	"gopkg.in/tylerb/graceful.v1"
)

//go:embed static
var staticFS embed.FS

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

	// TreePath is the path to the static tree website.
	TreePath = "/tree"

	// EndPointAuthTree is the endpoint for making treemap queries when
	// authorization is implemented.
	EndPointAuthTree = EndPointAuth + TreePath

	ErrNeedsAuth = Error("authentication must be enabled")

	defaultDir    = "/"
	defaultSplits = "2"
	stopTimeout   = 10 * time.Second
	devEnvKey     = "WRSTAT_SERVER_DEV"
	devEnvVal     = "1"
	unknown       = "#unknown"
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
	router         *gin.Engine
	tree           *dgut.Tree
	srv            *graceful.Server
	authGroup      *gin.RouterGroup
	authCB         AuthCallback
	uidToNameCache map[uint32]string
	gidToNameCache map[uint32]string
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
		router:         r,
		uidToNameCache: make(map[uint32]string),
		gidToNameCache: make(map[uint32]string),
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
// connections to close and the port to be available again. It also closes the
// database if you LoadDGUTDBs().
func (s *Server) Stop() {
	ch := s.srv.StopChan()
	s.srv.Stop(stopTimeout)
	<-ch

	if s.tree != nil {
		s.tree.Close()
	}
}

// LoadDGUTDBs loads the given dgut.db directories (as produced by one or more
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
// is a GET on /rest/v1/where or /rest/v1/auth/where.
func (s *Server) getWhere(c *gin.Context) {
	dir := c.DefaultQuery("dir", defaultDir)
	splits := c.DefaultQuery("splits", defaultSplits)

	filter, err := s.getFilter(c)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	dcss, err := s.tree.Where(dir, filter, convertSplitsValue(splits))
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	c.IndentedJSON(http.StatusOK, s.dcssToSummaries(dcss))
}

// getFilter extracts the user's filter requests, as restricted by their jwt,
// and returns a tree filter.
func (s *Server) getFilter(c *gin.Context) (*dgut.Filter, error) {
	groups := c.Query("groups")
	users := c.Query("users")
	types := c.Query("types")

	filterGIDs, err := s.restrictedGroups(c, groups)
	if err != nil {
		return nil, err
	}

	filterUIDs, err := s.userIDsFromNames(users)
	if err != nil {
		return nil, err
	}

	return makeTreeFilter(filterGIDs, filterUIDs, types)
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

// userIDsFromNames returns the user IDs that correspond to the given comma
// separated list of user names. This does not check the usernames stored in the
// JWT, because users are allowed to know about files owned by other users in
// the groups they belong to; security restrictions are purely based on the
// enforced restrictedGroups().
func (s *Server) userIDsFromNames(users string) ([]string, error) {
	ids, _, err := getWantedIDs(users, userNameToUID)
	if err != nil {
		return nil, err
	}

	return ids, nil
}

// userNameToUID converts user name to UID.
func userNameToUID(name string) (string, error) {
	u, err := user.Lookup(name)
	if err != nil {
		return "", err
	}

	return u.Uid, nil
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

// DirSummary holds nested file count and size information on a directory. It
// also holds which users and groups own files nested under the directory, and
// their file types. It differs from dgut.DirSummary in having string names for
// users, groups and types, instead of ids.
type DirSummary struct {
	Dir       string
	Count     uint64
	Size      uint64
	Users     []string
	Groups    []string
	FileTypes []string
}

// dcssToSummaries converts the given DCSs to our own DirSummary, the difference
// being we change the UIDs to usernames and the GIDs to group names. On failure
// to convert, the name will skipped.
func (s *Server) dcssToSummaries(dcss dgut.DCSs) []*DirSummary {
	summaries := make([]*DirSummary, len(dcss))

	for i, dds := range dcss {
		summaries[i] = s.dgutDStoSummary(dds)
	}

	return summaries
}

// dgutDStoSummary converts the given dgut.DirSummary to one of our DirSummary,
// basically just converting the *IDs to names.
func (s *Server) dgutDStoSummary(dds *dgut.DirSummary) *DirSummary {
	return &DirSummary{
		Dir:       dds.Dir,
		Count:     dds.Count,
		Size:      dds.Size,
		Users:     s.uidsToUsernames(dds.UIDs),
		Groups:    s.gidsToNames(dds.GIDs),
		FileTypes: s.ftsToNames(dds.FTs),
	}
}

// uidsToUsernames converts the given user IDs to usernames, sorted on the
// names.
func (s *Server) uidsToUsernames(uids []uint32) []string {
	return idsToSortedNames(uids, s.uidToNameCache, func(uid string) (string, error) {
		u, err := user.LookupId(uid)
		if err != nil {
			return "", err
		}

		return u.Username, nil
	})
}

// idsToSortedNames uses the given callback to convert the given ids to names
// (skipping if the cb errors), and sorts them. It caches results in the given
// map, avoiding the use of the cb if we already have the answer.
func idsToSortedNames(ids []uint32, cache map[uint32]string, cb func(string) (string, error)) []string {
	names := make([]string, len(ids))

	for i, id := range ids {
		name, found := cache[id]
		if found {
			names[i] = name

			continue
		}

		name, err := cb(fmt.Sprintf("%d", id))
		if err != nil {
			names[i] = unknown
		} else {
			names[i] = name
		}

		cache[id] = names[i]
	}

	names = removeUnknown(names)

	sort.Strings(names)

	return names
}

// removeUnknown does a no-allocation filter of slice to remove unknown entries.
func removeUnknown(slice []string) []string {
	filtered := slice[:0]

	for _, item := range slice {
		if item != unknown {
			filtered = append(filtered, item)
		}
	}

	return filtered
}

// gidsToNames converts the given unix group IDs to group names, sorted
// on the names.
func (s *Server) gidsToNames(gids []uint32) []string {
	return idsToSortedNames(gids, s.gidToNameCache, func(gid string) (string, error) {
		g, err := user.LookupGroupId(gid)
		if err != nil {
			return "", err
		}

		return g.Name, nil
	})
}

// ftsToNames converts the given file types to their names, sorted on the names.
func (s *Server) ftsToNames(fts []summary.DirGUTFileType) []string {
	names := make([]string, len(fts))

	for i, ft := range fts {
		names[i] = ft.String()
	}

	sort.Strings(names)

	return names
}

// AddTreePage adds the /tree static web page to the server, along with the
// /rest/v1/auth/tree endpoint. It only works if EnableAuth() has been called
// first.
func (s *Server) AddTreePage() error {
	if s.authGroup == nil {
		return ErrNeedsAuth
	}

	fsys := getStaticFS()

	s.router.StaticFS(TreePath, http.FS(fsys))

	s.router.NoRoute(func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/tree/tree.html")
	})

	s.authGroup.GET(TreePath, s.getTree)

	return nil
}

// getStaticFS returns an FS for the static files needed for the tree webpage.
// Returns embedded files by default, or a live view of the git repo files if
// env var WRSTAT_SERVER_DEV is set to 1.
func getStaticFS() fs.FS {
	var fsys fs.FS

	treeDir := "static/tree"

	if os.Getenv(devEnvKey) == devEnvVal {
		fsys = os.DirFS(treeDir)
	} else {
		fsys, _ = fs.Sub(staticFS, treeDir) //nolint:errcheck
	}

	return fsys
}

// TreeElement holds tree.DirInfo type information in a form suited to passing
// to the treemap web interface.
type TreeElement struct {
	Name        string         `json:"name"`
	Path        string         `json:"path"`
	Count       uint64         `json:"count"`
	Size        uint64         `json:"size"`
	Users       []string       `json:"users"`
	Groups      []string       `json:"groups"`
	FileTypes   []string       `json:"filetypes"`
	HasChildren bool           `json:"has_children"`
	Children    []*TreeElement `json:"children,omitempty"`
}

// getTree responds with the data needed by the tree web interface. LoadDGUTDB()
// must already have been called. This is called when there is a GET on
// /rest/v1/auth/tree.
func (s *Server) getTree(c *gin.Context) {
	path := c.DefaultQuery("path", "/")

	filter, err := s.getFilter(c)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	di, err := s.tree.DirInfo(path, filter)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	c.JSON(http.StatusOK, s.diToTreeElement(di, filter))
}

// diToTreeElement converts the given dgut.DirInfo to our own TreeElement. It
// has to do additional database queries to find out if di's children have
// children.
func (s *Server) diToTreeElement(di *dgut.DirInfo, filter *dgut.Filter) *TreeElement {
	te := s.ddsToTreeElement(di.Current)
	te.HasChildren = len(di.Children) > 0
	childElements := make([]*TreeElement, len(di.Children))

	for i, dds := range di.Children {
		childTE := s.ddsToTreeElement(dds)
		childTE.HasChildren = s.tree.DirHasChildren(dds.Dir, filter)
		childElements[i] = childTE
	}

	te.Children = childElements

	return te
}

// ddsToTreeElement converts a dgut.DirSummary to a TreeElement, but with no
// child info.
func (s *Server) ddsToTreeElement(dds *dgut.DirSummary) *TreeElement {
	return &TreeElement{
		Name:      filepath.Base(dds.Dir),
		Path:      dds.Dir,
		Count:     dds.Count,
		Size:      dds.Size,
		Users:     s.uidsToUsernames(dds.UIDs),
		Groups:    s.gidsToNames(dds.GIDs),
		FileTypes: s.ftsToNames(dds.FTs),
	}
}
