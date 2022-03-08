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

// package server provides a web server for a REST API.

package server

import (
	"net/http"
	"os/user"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/wtsi-ssg/wrstat/dgut"
	"github.com/wtsi-ssg/wrstat/summary"
)

const (
	defaultDir    = "/"
	defaultSplits = "2"
)

// Server is used to start a web server that provides a REST API to the dgut
// package's database.
type Server struct {
	router *gin.Engine
	tree   *dgut.Tree
}

// New creates a Server which can serve a REST API.
func New() *Server {
	gin.SetMode(gin.ReleaseMode)

	r := gin.New()
	r.Use(gin.Recovery()) // gin.Default() also includes gin.Logger()

	return &Server{
		router: r,
	}
}

// *** need an actual ListenAndServe method, ability to tell it a port to use,
// and an ability to test on an available port...

// LoadDGUTDB loads the given dgut.db (as produced by dgut.DB.Store()) so that
// getWhere() will work.
func (s *Server) LoadDGUTDB(path string) error {
	tree, err := dgut.NewTree(path)
	if err != nil {
		return err
	}

	s.tree = tree
	s.router.GET("/where", s.getWhere)

	return nil
}

// getWhere responds with a list of directory stats describing where data is on
// disks. LoadDGUTDB() must already have been called. This is called when there
// is a GET on /where.
func (s *Server) getWhere(c *gin.Context) {
	dir := c.DefaultQuery("dir", defaultDir)
	splits := c.DefaultQuery("splits", defaultSplits)
	groups := c.Query("groups")
	users := c.Query("users")
	types := c.Query("types")

	dcss, err := s.callWhere(dir, splits, groups, users, types)
	if err != nil {
		c.IndentedJSON(http.StatusBadRequest, nil)
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
