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

	// WhereEndPoint is the endpoint for making where queries.
	EndPointWhere = EndPointREST + "/where"

	defaultDir    = "/"
	defaultSplits = "2"
	stopTimeout   = 10 * time.Second
)

// Server is used to start a web server that provides a REST API to the dgut
// package's database, and a website that displays the information nicely.
type Server struct {
	router *gin.Engine
	tree   *dgut.Tree
	srv    *graceful.Server
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

// LoadDGUTDB loads the given dgut.db (as produced by dgut.DB.Store()) and adds
// the /rest/v1/where endpoint to the REST API.
//
// The /rest/v1/where endpoint can take the dir, splits, groups, users and types
// parameters, which correspond to arguments that dgut.Tree.Where() takes.
func (s *Server) LoadDGUTDB(path string) error {
	tree, err := dgut.NewTree(path)
	if err != nil {
		return err
	}

	s.tree = tree
	s.router.GET(EndPointWhere, s.getWhere)

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

	dcss, err := s.callWhere(dir, splits, groups, users, types)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	c.IndentedJSON(http.StatusOK, dcss)
}

// callWhere interprets string filters and passes them to tree.Where().
func (s *Server) callWhere(dir, splits, groups, users, types string) (dgut.DCSs, error) {
	filter, err := makeTreeFilter(groups, users, types)
	if err != nil {
		return nil, err
	}

	return s.tree.Where(dir, filter, convertSplitsValue(splits))
}

// makeTreeFilter creates a filter from string args.
func makeTreeFilter(groups, users, types string) (*dgut.Filter, error) {
	filter, err := makeTreeGroupFilter(groups)
	if err != nil {
		return nil, err
	}

	if err = addUsersToFilter(filter, users); err != nil {
		return nil, err
	}

	err = addTypesToFilter(filter, types)

	return filter, err
}

// makeTreeGroupFilter creates a filter for groups.
func makeTreeGroupFilter(groups string) (*dgut.Filter, error) {
	if groups == "" {
		return &dgut.Filter{}, nil
	}

	gnames := strings.Split(groups, ",")
	gids := make([]uint32, len(gnames))

	for i, name := range gnames {
		group, err := user.LookupGroup(name)
		if err != nil {
			return nil, err
		}

		// no error is possible here, with the number string coming from an OS
		// lookup.
		//nolint:errcheck
		gid, _ := strconv.ParseUint(group.Gid, 10, 32)

		gids[i] = uint32(gid)
	}

	return &dgut.Filter{GIDs: gids}, nil
}

// addUsersToFilter adds a filter for users to the given filter.
func addUsersToFilter(filter *dgut.Filter, users string) error {
	if users == "" {
		return nil
	}

	unames := strings.Split(users, ",")
	uids := make([]uint32, len(unames))

	for i, name := range unames {
		user, err := user.Lookup(name)
		if err != nil {
			return err
		}

		// no error is possible here, with the number string coming from an OS
		// lookup.
		//nolint:errcheck
		uid, _ := strconv.ParseUint(user.Uid, 10, 32)

		uids[i] = uint32(uid)
	}

	filter.UIDs = uids

	return nil
}

// addTypesToFilter adds a filter for types to the given filter.
func addTypesToFilter(filter *dgut.Filter, types string) error {
	if types == "" {
		return nil
	}

	tnames := strings.Split(types, ",")
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
