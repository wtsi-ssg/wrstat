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

package summary

import (
	"fmt"
	"io/fs"
	"os"
	"os/user"
	"path/filepath"
	"sort"
	"strconv"
	"syscall"
)

type Error string

func (e Error) Error() string { return string(e) }

const errNotUnix = Error("file info Sys() was not a *syscall.Stat_t; only unix is supported")

// dirStore is a sortable map with directory paths as keys and summaries as
// values.
type dirStore map[string]*summary

// add will auto-vivify a summary for the given directory path and call
// add(size) on it.
func (store dirStore) add(path string, size int64) {
	s, ok := store[path]
	if !ok {
		s = &summary{}
		store[path] = s
	}

	s.add(size)
}

// sort returns a slice of our summary values, sorted by our directory path
// keys which are also returned.
func (store dirStore) sort() ([]string, []*summary) {
	keys := make([]string, len(store))
	i := 0

	for k := range store {
		keys[i] = k
		i++
	}

	sort.Strings(keys)

	s := make([]*summary, len(store))

	for i, k := range keys {
		s[i] = store[k]
	}

	return keys, s
}

// groupStore is a sortable map of gid to dirStore.
type groupStore map[uint32]dirStore

// getDirStore auto-vivifies a dirStore for the given gid and returns it.
func (store groupStore) getDirStore(gid uint32) dirStore {
	dStore, ok := store[gid]
	if !ok {
		dStore = make(dirStore)
		store[gid] = dStore
	}

	return dStore
}

// sort returns a slice of our dirStore values, sorted by our gid keys converted
// to group names, which are also returned. Returns an error if a gid couldn't
// be converted to a group name.
//
// If you will be sorting multiple different groupStores, supply them all the
// same gidLookupCache which is used to minimise gid to name lookups.
func (store groupStore) sort(gidLookupCache map[uint32]string) ([]string, []dirStore, error) {
	byGroupName := make(map[string]dirStore)

	for gid, dStore := range store {
		name, err := gidToName(gid, gidLookupCache)
		if err != nil {
			return nil, nil, err
		}

		byGroupName[name] = dStore
	}

	keys := make([]string, len(byGroupName))
	i := 0

	for k := range byGroupName {
		keys[i] = k
		i++
	}

	sort.Strings(keys)

	s := make([]dirStore, len(byGroupName))

	for i, k := range keys {
		s[i] = byGroupName[k]
	}

	return keys, s, nil
}

// gidToName converts gid to group name, using the given cache to avoid lookups.
func gidToName(gid uint32, cache map[uint32]string) (string, error) {
	if name, ok := cache[gid]; ok {
		return name, nil
	}

	g, err := user.LookupGroupId(strconv.Itoa(int(gid)))
	if err != nil {
		return "", err
	}

	cache[gid] = g.Name

	return g.Name, nil
}

// userStore is a sortable map of uid to groupStore.
type userStore map[uint32]groupStore

// DirStore auto-vivifies an entry in our store for the given uid and gid and
// returns it.
func (store userStore) DirStore(uid, gid uint32) dirStore {
	return store.getGroupStore(uid).getDirStore(gid)
}

// getGroupStore auto-vivifies a groupStore for the given uid and returns it.
func (store userStore) getGroupStore(uid uint32) groupStore {
	gStore, ok := store[uid]
	if !ok {
		gStore = make(groupStore)
		store[uid] = gStore
	}

	return gStore
}

// sort returns a slice of our groupStore values, sorted by our uid keys
// converted to user names, which are also returned. Returns an error if a uid
// couldn't be converted to a user name.
func (store userStore) sort() ([]string, []groupStore, error) {
	byUserName := make(map[string]groupStore)

	for uid, gids := range store {
		u, err := user.LookupId(strconv.Itoa(int(uid)))
		if err != nil {
			return nil, nil, err
		}

		byUserName[u.Name] = gids
	}

	keys := make([]string, len(byUserName))
	i := 0

	for k := range byUserName {
		keys[i] = k
		i++
	}

	sort.Strings(keys)

	s := make([]groupStore, len(byUserName))

	for i, k := range keys {
		s[i] = byUserName[k]
	}

	return keys, s, nil
}

// Usergroup is used to summarise file stats by user and group.
type Usergroup struct {
	store userStore
}

// New returns a Usergroup.
func New() *Usergroup {
	return &Usergroup{
		store: make(userStore),
	}
}

// Add is a github.com/wtsi-ssg/wrstat/stat Operation. It will break path in to
// its directories and add the file size and increment the file count to each,
// summed for the info's user and group. If path is a directory, it is ignored.
func (u *Usergroup) Add(path string, info fs.FileInfo) error {
	if info.IsDir() {
		return nil
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errNotUnix
	}

	dStore := u.store.DirStore(stat.Uid, stat.Gid)

	addForEachDir(path, info.Size(), dStore)

	return nil
}

// addForEachDir breaks path into each directory and adds the size to the
// summary in the store, creating one for each dir if necessary.
func addForEachDir(path string, size int64, store dirStore) {
	dir := filepath.Dir(path)

	for {
		if dir == "/" || dir == "." {
			return
		}

		store.add(dir, size)

		dir = filepath.Dir(dir)
	}
}

// Output will write summary information for all the paths previously added. The
// format is (tab separated):
//
// username group directory filecount filesize
//
// usernames, groups and directories are sorted.
//
// Returns an error on failure to write, or if username or group can't be
// determined from the uids and gids in the added file info.
func (u *Usergroup) Output(output *os.File) error {
	users, gStores, err := u.store.sort()
	if err != nil {
		return err
	}

	gidLookupCache := make(map[uint32]string)

	for i, username := range users {
		err = outputGroupDirectorySummariesForUser(output, username, gStores[i], gidLookupCache)
		if err != nil {
			return err
		}
	}

	return nil
}

// outputGroupDirectorySummariesForUser sortes the groups for this user and
// calls outputDirectorySummariesForGroup.
func outputGroupDirectorySummariesForUser(output *os.File, username string,
	gStore groupStore, gidLookupCache map[uint32]string) error {
	groupnames, dStores, err := gStore.sort(gidLookupCache)
	if err != nil {
		return err
	}

	for i, groupname := range groupnames {
		err = outputDirectorySummariesForGroup(output, username, groupname, dStores[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// outputDirectorySummariesForGroup sorts the directories for this group and
// does the actual output of all the summary information.
func outputDirectorySummariesForGroup(output *os.File, username, groupname string, dStore dirStore) error {
	dirs, summaries := dStore.sort()

	for i, s := range summaries {
		_, errw := output.WriteString(fmt.Sprintf("%s\t%s\t%s\t%d\t%d\n", username, groupname, dirs[i], s.count, s.size))
		if errw != nil {
			return errw
		}
	}

	return nil
}
