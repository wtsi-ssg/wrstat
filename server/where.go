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
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/wtsi-ssg/wrstat/v4/basedirs"
	"github.com/wtsi-ssg/wrstat/v4/internal/split"
)

const defaultSplits = 2

// getWhere responds with a list of directory stats describing where data is on
// disks. LoadDGUTDB() must already have been called. This is called when there
// is a GET on /rest/v1/where or /rest/v1/auth/where.
func (s *Server) getWhere(c *gin.Context) {
	dir := c.DefaultQuery("dir", defaultDir)
	splits := c.DefaultQuery("splits", strconv.FormatUint(basedirs.DefaultSplits, 10))

	filter, err := s.makeRestrictedFilterFromContext(c)
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	s.treeMutex.Lock()
	defer s.treeMutex.Unlock()

	dcss, err := s.tree.Where(dir, filter, convertSplitsValue(splits))
	if err != nil {
		c.AbortWithError(http.StatusBadRequest, err) //nolint:errcheck

		return
	}

	c.IndentedJSON(http.StatusOK, s.dcssToSummaries(dcss))
}

// convertSplitsValue returns a split.SplitFn that always returns the value
// specified. If the given value fails to be parsed as a Uint, the default value
// of 2 will be used.
func convertSplitsValue(splits string) split.SplitFn {
	splitsN, err := strconv.ParseUint(splits, 10, 8)
	if err != nil {
		return split.SplitsToSplitFn(defaultSplits)
	}

	return split.SplitsToSplitFn(int(splitsN))
}
