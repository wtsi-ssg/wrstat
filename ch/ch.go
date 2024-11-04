/*******************************************************************************
 * Copyright (c) 2021 Genome Research Ltd.
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

// package ch is used to do chmod and chown on certain files, to correct for
// group and user permissions and ownership being wrong.

package ch

import (
	"errors"
	"io/fs"
	"os"
	"os/user"
	"strconv"
	"syscall"

	"github.com/hashicorp/go-multierror"
	"github.com/inconshreveable/log15"
)

// Ch is used to chmod and chown files such that they match their desired group.
type Ch struct {
	rs     *RulesStore
	logger log15.Logger
}

// New returns a Ch what will use your RulesStore to see what work needs to
// be done on the paths this Ch will receive when Do() is called on it.
//
// Changes made will be logged to the given logger.
func New(rs *RulesStore, logger log15.Logger) *Ch {
	return &Ch{
		rs:     rs,
		logger: logger,
	}
}

// Do is a github.com/wtsi-ssg/wrstat/stat Operation that passes path to our
// PathCheck callback, and if it returns true, does the following chmod and
// chown-type behaviours, making use of the supplied Lstat info to avoid doing
// unnecessary repeated work:
//
// 1. Ensures that the GID of the path is the returned GID.
// 2. If path is a directory, ensures it has setgid applied (group sticky).
// 3. Ensures that User execute permission is set if group execute was set.
// 4. Ensures that group permissions match user permissions.
// 5. Forces user and group read and writeability.
//
// Any errors are returned without logging them, except for "not exists" errors
// which are silently ignored since these are expected.
//
// Any changes we do on disk are logged to our logger.
func (c *Ch) Do(path string, info fs.FileInfo) error {
	rule := c.rs.Get(path)
	if rule == nil {
		return nil
	}

	chain := &chain{}

	chain.Call(func() error {
		return c.chown(rule, path, info)
	})

	chain.Call(func() error {
		return c.chmod(rule, path, info)
	})

	return chain.merr
}

// chain lets you call a chain of functions and combine their errors.
type chain struct {
	merr error
	stop bool
}

// Call will run your function and append any error to our merr, except for
// os.ErrNotExist, which instead result in future Call()s to no-op.
func (c *chain) Call(f func() error) {
	if c.stop {
		return
	}

	if err := f(); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			c.stop = true

			return
		}

		c.merr = multierror.Append(c.merr, err)
	}
}

func (c *Ch) chown(rule *Rule, path string, info fs.FileInfo) error {
	currentUID, currentGID := getIDsFromFileInfo(info)
	desiredUID := rule.DesiredUser(currentUID)
	desiredGID := rule.DesiredGroup(currentGID)

	if currentUID == desiredUID && currentGID == desiredGID {
		return nil
	}

	if err := os.Lchown(path, int(desiredUID), int(desiredGID)); err != nil {
		return err
	}

	return c.logChown(path, currentUID, desiredUID, currentGID, desiredGID)
}

func (c *Ch) logChown(path string, currentUID, desiredUID, currentGID, desiredGID uint32) error {
	origUName, err := userName(int(currentUID))
	if err != nil {
		return err
	}

	newUName, err := userName(int(desiredUID))
	if err != nil {
		return err
	}

	origGName, err := groupName(int(currentGID))
	if err != nil {
		return err
	}

	newGName, err := groupName(int(desiredGID))
	if err != nil {
		return err
	}

	c.logger.Info("changed ownership", "path", path,
		"origUser", origUName, "newUser", newUName,
		"origGroup", origGName, "newGroup", newGName)

	return nil
}

// getIDsFromFileInfo extracts the UID and GID from a FileInfo. NB: this will
// only work on linux.
func getIDsFromFileInfo(info fs.FileInfo) (uint32, uint32) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0
	}

	return stat.Uid, stat.Gid
}

// userName returns the username of the user with the given UID.
func userName(uid int) (string, error) {
	u, err := user.LookupId(strconv.Itoa(uid))
	if err != nil {
		return "", err
	}

	return u.Username, err
}

// groupName returns the name of the group with the given GID.
func groupName(gid int) (string, error) {
	g, err := user.LookupGroupId(strconv.Itoa(gid))
	if err != nil {
		return "", err
	}

	return g.Name, err
}

func (c *Ch) chmod(rule *Rule, path string, info fs.FileInfo) error {
	if info.Mode()&fs.ModeSymlink == fs.ModeSymlink {
		return nil
	}

	currentPerms := info.Mode()

	var desiredPerms fs.FileMode

	if info.IsDir() {
		desiredPerms = rule.DesiredDirPerms(currentPerms)
	} else {
		desiredPerms = rule.DesiredFilePerms(currentPerms)
	}

	if currentPerms == desiredPerms {
		return nil
	}

	if err := os.Chmod(path, desiredPerms); err != nil {
		return err
	}

	c.logger.Info("set permissions", "path", path, "old", currentPerms, "new", desiredPerms)

	return nil
}
