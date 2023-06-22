/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
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
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/wtsi-ssg/wrstat/v4/basedirs"
)

// LoadBasedirsDB loads the given basedirs.db file (as produced by
// basedirs.CreateDatabase()) and makes use of the given owners file (a
// gid,owner csv) and adds the following GET endpoints to the REST API:
//
// /rest/v1/basedirs/usage/groups
// /rest/v1/basedirs/usage/users
// /rest/v1/basedirs/subdirs/group
// /rest/v1/basedirs/subdirs/user
// /rest/v1/basedirs/history
//
// If you call EnableAuth() first, then these endpoints will be secured and be
// available at /rest/v1/auth/basedirs/*.
//
// The subdir endpoints require id (gid or uid) and basedir parameters.
// The history endpoint requires a gid and path (can be basedir, actually a
// mountpoint) parameter.
func (s *Server) LoadBasedirsDB(dbPath, ownersPath string) error {
	s.basedirsMutex.Lock()
	defer s.basedirsMutex.Unlock()

	bd, err := basedirs.NewReader(dbPath, ownersPath)
	if err != nil {
		return err
	}

	s.basedirs = bd
	s.basedirsPath = dbPath

	authGroup := s.AuthRouter()

	if authGroup == nil {
		s.Router().GET(EndPointBasedirUsageGroup, s.getBasedirsGroupUsage)
		s.Router().GET(EndPointBasedirUsageUser, s.getBasedirsUserUsage)
	} else {
		authGroup.GET(basedirsGroupUsagePath, s.getBasedirsGroupUsage)
		authGroup.GET(basedirsUserUsagePath, s.getBasedirsUserUsage)
	}

	return nil
}

func (s *Server) getBasedirsGroupUsage(c *gin.Context) {
	s.getBasedirs(c, func() (any, error) {
		return s.basedirs.GroupUsage()
	})
}

// getBasedirs responds with the output of your callback in JSON format.
// LoadBasedirsDB() must already have been called.
//
// This is called when there is a GET on /rest/v1/basedirs/* or
// /rest/v1/authbasedirs/*.
func (s *Server) getBasedirs(c *gin.Context, cb func() (any, error)) {
	s.basedirsMutex.RLock()
	defer s.basedirsMutex.RUnlock()

	result, err := cb()
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	c.IndentedJSON(http.StatusOK, result)
}

func (s *Server) getBasedirsUserUsage(c *gin.Context) {
	s.getBasedirs(c, func() (any, error) {
		return s.basedirs.UserUsage()
	})
}
