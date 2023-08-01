/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
 *          Sendu Bala <sb10@sanger.ac.uk>
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

package ch

import (
	"io/fs"
	"os"
	"os/user"
	"strconv"
	"syscall"
)

type setAction uint8

const (
	nothing setAction = iota
	set
	unset
	matchSet
)

func parseAction(action byte) setAction {
	switch action {
	case '*':
		return nothing
	case '-':
		return unset
	case '^':
		return matchSet
	default:
		return set
	}
}

func (s setAction) toFileMode(currentMode fs.FileMode, match bool) fs.FileMode {
	switch s {
	case nothing:
		return currentMode & 1
	case set:
		return 1
	case matchSet:
		if match {
			return 1
		}
	}

	return 0
}

type Perms struct {
	read    setAction
	write   setAction
	execute setAction
	sticky  bool
}

func parsePerms(rwx string) Perms {
	return Perms{
		read:    parseAction(rwx[0]),
		write:   parseAction(rwx[1]),
		execute: parseAction(rwx[2]),
		sticky:  rwx[2] == 's',
	}
}

func (p Perms) toFileMode(currentMode fs.FileMode, readBit, writeBit, executeBit bool) fs.FileMode {
	var mode fs.FileMode

	mode |= p.read.toFileMode(currentMode>>2, readBit)
	mode <<= 1
	mode |= p.write.toFileMode(currentMode>>1, writeBit)
	mode <<= 1
	mode |= p.execute.toFileMode(currentMode, executeBit)

	return mode
}

type UGOPerms struct {
	user, group, other Perms
}

func parseTSVPerms(perms string) *UGOPerms {
	return &UGOPerms{
		user:  parsePerms(perms[:3]),
		group: parsePerms(perms[3:6]),
		other: parsePerms(perms[6:]),
	}
}

func (u *UGOPerms) toFileMode(currentMode fs.FileMode) fs.FileMode {
	var mode fs.FileMode

	readBit := (currentMode & 0444) > 0
	writeBit := (currentMode & 0222) > 0
	executeBit := (currentMode & 0111) > 0

	mode |= u.user.toFileMode(currentMode>>6, readBit, writeBit, executeBit)
	mode <<= 3
	mode |= u.group.toFileMode(currentMode>>3, readBit, writeBit, executeBit)
	mode <<= 3
	mode |= u.other.toFileMode(currentMode, readBit, writeBit, executeBit)

	if u.group.sticky {
		mode |= fs.ModeSticky
	}

	return mode
}

type Rule struct {
	uid, gid                uint32
	changeUser, changeGroup bool
	filePerms, dirPerms     *UGOPerms
}

func (r *Rule) DesiredUser(uid uint32) uint32 {
	if !r.changeUser {
		return uid
	}

	return r.uid
}

func (r *Rule) DesiredGroup(gid uint32) uint32 {
	if !r.changeGroup {
		return gid
	}

	return r.gid
}

func (r *Rule) DesiredFilePerms(perms fs.FileMode) fs.FileMode {
	return r.filePerms.toFileMode(perms)
}

func (r *Rule) DesiredDirPerms(perms fs.FileMode) fs.FileMode {
	return r.dirPerms.toFileMode(perms)
}

type nameToIDFunc func(string) (uint32, error)

// RulesStore is
type RulesStore struct {
	ppt                 *pathPrefixTree
	rules               map[string]*Rule
	userToUIDFunc       nameToIDFunc
	groupToGIDFunc      nameToIDFunc
	dirToUserOwnerFunc  nameToIDFunc
	dirToGroupOwnerFunc nameToIDFunc
}

func NewRulesStore() *RulesStore {
	return &RulesStore{
		ppt:                 newPathPrefixTree(),
		rules:               make(map[string]*Rule),
		userToUIDFunc:       defaultUserToUID,
		groupToGIDFunc:      defaultGroupToGID,
		dirToUserOwnerFunc:  defaultDirToUser,
		dirToGroupOwnerFunc: defaultDirToGroup,
	}
}

func defaultUserToUID(userName string) (uint32, error) {
	u, err := user.Lookup(userName)
	if err != nil {
		return 0, err
	}

	uid, err := strconv.Atoi(u.Uid)
	if err != nil {
		return 0, err
	}

	return uint32(uid), nil
}

func defaultGroupToGID(groupName string) (uint32, error) {
	g, err := user.LookupGroup(groupName)
	if err != nil {
		return 0, err
	}

	gid, err := strconv.Atoi(g.Gid)
	if err != nil {
		return 0, err
	}

	return uint32(gid), nil
}

func defaultDirToUser(directory string) (uint32, error) {
	stat, err := directoryStat(directory)
	if err != nil {
		return 0, err
	}

	return stat.Uid, nil
}

func directoryStat(directory string) (*syscall.Stat_t, error) {
	info, err := os.Lstat(directory)
	if err != nil {
		return nil, err
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return nil, fs.ErrInvalid
	}

	return stat, nil
}

func defaultDirToGroup(directory string) (uint32, error) {
	stat, err := directoryStat(directory)
	if err != nil {
		return 0, err
	}

	return stat.Gid, nil
}

func (r *RulesStore) FromTSV(tsvReader *TSVReader) (*RulesStore, error) {
	for tsvReader.Next() {
		cols := tsvReader.Columns()
		r.ppt.addDirectory(cols[0])

		rule := new(Rule)

		var err error

		if cols[1] == "^" {
			rule.uid, err = r.dirToUserOwnerFunc(cols[0])
			if err != nil {
				return r, err
			}

			rule.changeUser = true
		} else if cols[1] != "*" {
			rule.uid, err = r.userToUIDFunc(cols[1])
			if err != nil {
				return r, err
			}

			rule.changeUser = true
		}

		if cols[2] == "^" {
			rule.gid, err = r.dirToGroupOwnerFunc(cols[0])
			if err != nil {
				return r, err
			}

			rule.changeGroup = true
		} else if cols[2] != "*" {
			rule.gid, err = r.groupToGIDFunc(cols[2])
			if err != nil {
				return r, err
			}

			rule.changeGroup = true
		}

		rule.filePerms = parseTSVPerms(cols[3])
		rule.dirPerms = parseTSVPerms(cols[4])
		r.rules[cols[0]] = rule
	}

	return r, tsvReader.Error()
}

func (r *RulesStore) Get(path string) *Rule {
	prefix, found := r.ppt.longestPrefix(path)
	if !found {
		return nil
	}

	return r.rules[prefix]
}
