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
	"sort"
	"strconv"
	"syscall"
)

// userToSummary is a sortable map with uids as keys and summaries as values.
type userToSummaryStore map[uint32]*summary

// add will auto-vivify a summary for the given uid and call add(size) on it.
func (store userToSummaryStore) add(uid uint32, size int64) {
	s, ok := store[uid]
	if !ok {
		s = &summary{}
		store[uid] = s
	}

	s.add(size)
}

// sort returns a slice of our summary values, sorted by our uid keys converted
// to user names, which are also returned. Returns an error if a uid couldn't be
// converted to a user name.
//
// If you will be sorting multiple different userToSummaryStores, supply them
// all the same uidLookupCache which is used to minimise uid to name lookups.
func (store userToSummaryStore) sort(uidLookupCache map[uint32]string) ([]string, []*summary, error) {
	byUserName := make(map[string]*summary)

	for uid, summary := range store {
		name, err := uidToName(uid, uidLookupCache)
		if err != nil {
			return nil, nil, err
		}

		byUserName[name] = summary
	}

	keys := make([]string, len(byUserName))
	i := 0

	for k := range byUserName {
		keys[i] = k
		i++
	}

	sort.Strings(keys)

	s := make([]*summary, len(byUserName))

	for i, k := range keys {
		s[i] = byUserName[k]
	}

	return keys, s, nil
}

// uidToName converts uid to username, using the given cache to avoid lookups.
func uidToName(uid uint32, cache map[uint32]string) (string, error) {
	return cachedIDToName(uid, cache, getUserName)
}

// getUserName returns the username of the user given uid.
func getUserName(id string) (string, error) {
	u, err := user.LookupId(id)
	if err != nil {
		return "", err
	}

	return u.Username, nil
}

// groupToUserStore is a sortable map of gid to userToSummaryStore.
type groupToUserStore map[uint32]userToSummaryStore

// getGroupToUserStore auto-vivifies a groupToUser for the given gid and returns it.
func (store groupToUserStore) getGroupToUserStore(gid uint32) userToSummaryStore {
	uStore, ok := store[gid]
	if !ok {
		uStore = make(userToSummaryStore)
		store[gid] = uStore
	}

	return uStore
}

// sort returns a slice of our userToSummaryStore values, sorted by our gid keys
// converted to unix group names, which are also returned. Returns an error if a
// gid couldn't be converted to a name.
func (store groupToUserStore) sort() ([]string, []userToSummaryStore, error) {
	byGroupName := make(map[string]userToSummaryStore)

	for gid, uStore := range store {
		g, err := user.LookupGroupId(strconv.Itoa(int(gid)))
		if err != nil {
			return nil, nil, err
		}

		byGroupName[g.Name] = uStore
	}

	keys := make([]string, len(byGroupName))
	i := 0

	for k := range byGroupName {
		keys[i] = k
		i++
	}

	sort.Strings(keys)

	s := make([]userToSummaryStore, len(byGroupName))

	for i, k := range keys {
		s[i] = byGroupName[k]
	}

	return keys, s, nil
}

// GroupUser is used to summarise file stats by group and user.
type GroupUser struct {
	store groupToUserStore
}

// NewByGroupUser returns a GroupUser.
func NewByGroupUser() *GroupUser {
	return &GroupUser{
		store: make(groupToUserStore),
	}
}

// Add is a github.com/wtsi-ssg/wrstat/stat Operation. It will add the file size
// and increment the file count summed for the info's group and user. If path is
// a directory, it is ignored.
func (g *GroupUser) Add(path string, info fs.FileInfo) error {
	if info.IsDir() {
		return nil
	}

	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return errNotUnix
	}

	g.store.getGroupToUserStore(stat.Gid).add(stat.Uid, info.Size())

	return nil
}

// Output will write summary information for all the paths previously added. The
// format is (tab separated):
//
// group username filecount filesize
//
// group and username are sorted, and there is a special username "all" to give
// total filecount and filesize for all users that wrote files in that group.
//
// Returns an error on failure to write, or if username or group can't be
// determined from the uids and gids in the added file info. output is closed
// on completion.
func (g *GroupUser) Output(output *os.File) error {
	groups, uStores, err := g.store.sort()
	if err != nil {
		return err
	}

	uidLookupCache := make(map[uint32]string)

	for i, groupname := range groups {
		err = outputUserSummariesForGroup(output, groupname, uStores[i], uidLookupCache)
		if err != nil {
			return err
		}
	}

	return output.Close()
}

// outputUserSummariesForGroup sorts the users for this group and outputs the
// summary information.
func outputUserSummariesForGroup(output *os.File, groupname string,
	uStore userToSummaryStore, uidLookupCache map[uint32]string) error {
	usernames, summaries, err := uStore.sort(uidLookupCache)
	if err != nil {
		return err
	}

	for i, s := range summaries {
		_, errw := output.WriteString(fmt.Sprintf("%s\t%s\t%d\t%d\n", groupname, usernames[i], s.count, s.size))
		if errw != nil {
			return errw
		}
	}

	return nil
}
