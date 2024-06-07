/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
 *   Michael Woolnough <mw31@sanger.ac.uk>
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

package basedirs

import (
	"os/user"
	"strconv"
	"sync"
)

var gcmu, ucmu sync.RWMutex //nolint:gochecknoglobals

type GroupCache map[uint32]string

func (g GroupCache) GroupName(gid uint32) string {
	gcmu.RLock()
	groupName, ok := g[gid]
	gcmu.RUnlock()

	if ok {
		return groupName
	}

	groupStr := strconv.FormatUint(uint64(gid), 10)

	group, err := user.LookupGroupId(groupStr)
	if err == nil {
		groupStr = group.Name
	}

	gcmu.Lock()
	g[gid] = groupStr
	gcmu.Unlock()

	return groupStr
}

type UserCache map[uint32]string

func (u UserCache) UserName(uid uint32) string {
	ucmu.RLock()
	userName, ok := u[uid]
	ucmu.RUnlock()

	if ok {
		return userName
	}

	userStr := strconv.FormatUint(uint64(uid), 10)

	uu, err := user.LookupId(userStr)
	if err == nil {
		userStr = uu.Username
	}

	ucmu.Lock()
	u[uid] = userStr
	ucmu.Unlock()

	return userStr
}
