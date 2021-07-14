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

package ch

import (
	"os/user"
	"regexp"
	"strconv"
	"strings"

	"github.com/inconshreveable/log15"
	"gopkg.in/yaml.v2"
)

// regexp* consts relate to the groups matched in our main regexp.
const (
	regexpSubgroups = 4
	regexpDirPart   = 2
	regexpGroupPart = 3
)

const badUnixGroup = -1

type Error string

func (e Error) Error() string { return string(e) }

const errInvalidYAML = Error("YAML is missing properties")

// GIDFromSubDir provides a PathChecker that can decide if a path should be
// looked at based on matching a prefix followed by a certain sub directory
// (lookupDir or directDir), and also decides what the GID of that path should
// be based on the sub dir of that sub dir.
//
// For subdirs of lookup directories, it that converts from directory name to
// desired unix group name using the lookup, then gets the GID for that unix
// group.
//
// For subdirs of direct directories, it treats the directory name as a unix
// group name, and gets the GID of that unix group. You can supply exceptions
// where your own GID is used instead.
//
// With prefixes:
// "/disk1", "/disk2/sub", "/disk3"
//
// And a lookupDir of "teams" and a directDir of "projects"
//
// And lookup:
// "ay": "dee"
//
// And exceptions:
// "cee": 12345
//
// Given the paths:
// 1) /disk1/teams/ay/file1.txt
// 2) /disk2/sub/projects/bee/file2.txt
// 3) /disk2/sub/projects/cee/file3.txt
// 4) /disk3/file4.txt
// 5) /disk1/teams/new/file5.txt
// 6) /disk2/sub/projects/not_a_unix_group_name/file6.txt
//
// The .PathChecker() will return the following for each file:
// 1) true, [gid of unix group dee]
// 2) true, [gid of unix group bee]
// 3) true, 12345
// 4) false, n/a
// 5) false, n/a [and logs an error that "new" wasn't a known lookup]
// 6) false, n/a [and logs an error that "not_a_unix_group_name" has no GID].
type GIDFromSubDir struct {
	r          *regexp.Regexp
	lookupDir  string
	directDir  string
	lookup     map[string]int
	exceptions map[string]int
	logger     log15.Logger
}

// NewGIDFromSubDir returns a GIDFromSubDir.
//
// prefixes are absolute paths to directories that our PathChecker will return
// true for if the path matches a prefix and also contains a subdirectory named
// [lookupDir] or [directDir], and the path is for an entry located within a
// further subdirectory of that.
//
// lookupDir is the name of a subdirectory of the prefix paths that contains
// further subdirectories that are keys in the given lookup map. The values in
// the map are the desired unix group names, which will be converted to GIDs.
//
// directDir is the name of a subdirectory of the prefix paths that contains
// further subdirectories that are named after unix group names. Or their name
// is a key in the exceptions map, and the corresponding value will be the GID
// used. NB. unix group name to GID lookups are cached in the supplied
// exceptions map.
//
// If lookupDir contains a subdirectory that isn't in your lookup, or directDir
// contains a subdirectory that isn't in your exceptions and isn't a unix group
// name, these issues are logged to the given logger, and the PathChecker will
// return false.
func NewGIDFromSubDir(prefixes []string, lookupDir string, lookup map[string]string,
	directDir string, exceptions map[string]int, logger log15.Logger) (*GIDFromSubDir, error) {
	r := createPrefixRegex(prefixes, lookupDir, directDir)

	gidLookup, err := createGIDLookup(lookup)
	if err != nil {
		return nil, err
	}

	return &GIDFromSubDir{
		r:          r,
		lookupDir:  lookupDir,
		lookup:     gidLookup,
		directDir:  directDir,
		exceptions: exceptions,
		logger:     logger,
	}, nil
}

// createPrefixRegex creates a regexp that matches on given prefixes followed by
// one of lookupDir or directDir, followed by another subdir, and has each as
// capture groups.
func createPrefixRegex(prefixes []string, lookupDir, directDir string) *regexp.Regexp {
	escapedPrefixes := make([]string, len(prefixes))
	for i, prefix := range prefixes {
		escapedPrefixes[i] = regexp.QuoteMeta(prefix)
	}

	expr := `^(` + strings.Join(escapedPrefixes, `|`) + `)\/(` +
		regexp.QuoteMeta(lookupDir) + `|` +
		regexp.QuoteMeta(directDir) + `)\/([^/]+)\/.*$`

	return regexp.MustCompile(expr)
}

// createGIDLookup takes the given lookup values (treating them as unix group
// names), converts them to GIDs, and returns a new map with the same keys.
func createGIDLookup(lookup map[string]string) (map[string]int, error) {
	gl := make(map[string]int, len(lookup))

	for key, name := range lookup {
		gid, err := gidFromName(name)
		if err != nil {
			return nil, err
		}

		gl[key] = gid
	}

	return gl, nil
}

// gidFromName looks up unix group by name and returns the GID as an int.
func gidFromName(group string) (int, error) {
	g, err := user.LookupGroup(group)
	if err != nil {
		return 0, err
	}

	return strconv.Atoi(g.Gid)
}

// PathChecker returns a PathChecker that can be used with a Ch.
func (f *GIDFromSubDir) PathChecker() PathChecker {
	return func(path string) (change bool, gid int) {
		parts := f.r.FindStringSubmatch(path)
		if len(parts) != regexpSubgroups {
			return
		}

		if parts[regexpDirPart] == f.lookupDir {
			gid = f.lookupGID(parts[regexpGroupPart])
		} else {
			gid = f.directGID(parts[regexpGroupPart])
		}

		change = gid != badUnixGroup

		return
	}
}

// lookupGID returns the GID corresponding to the unix group value in our
// lookup with the given key.
func (f *GIDFromSubDir) lookupGID(key string) int {
	if gid, set := f.lookup[key]; set {
		return gid
	}

	f.logger.Warn("subdir not in group lookup", "dir", key)

	return badUnixGroup
}

// directGID returns the GID corresponding to the given unix group, unless group
// is in our exceptions map, in which case that value is returned.
func (f *GIDFromSubDir) directGID(group string) int {
	if gid, set := f.exceptions[group]; set {
		return gid
	}

	gid, err := gidFromName(group)
	if err != nil {
		f.logger.Warn("subdir not a unix group name", "dir", group)

		gid = badUnixGroup
	}

	f.exceptions[group] = gid

	return gid
}

// yamlForGIDFromSubDir is the struct we decode YAML in to during
// NewGIDFromSubDirFromYAML().
type yamlForGIDFromSubDir struct {
	Prefixes   []string
	LookupDir  string `yaml:"lookupDir"`
	DirectDir  string `yaml:"directDir"`
	Lookup     map[string]string
	Exceptions map[string]int
}

// valid checks that required fields have been set.
func (y *yamlForGIDFromSubDir) valid() bool {
	if len(y.Prefixes) == 0 || y.LookupDir == "" || y.DirectDir == "" || len(y.Lookup) == 0 {
		return false
	}

	return true
}

// NewGIDFromSubDirFromYAML is like NewGIDFromSubDir, but instead of supplying
// all the different args, you supply it YAML data in the following format:
//
// prefixes: ["/disk1", "/disk2/sub", "/disk3"]
// lookupDir: subdir_name_of_prefixes_that_contains_subdirs_in_lookup
// lookup:
//   foo: unix_group_name
// directDir: subdir_of_prefixes_with_unix_group_or_exception_subdirs
// exceptions:
//   bar: GID
func NewGIDFromSubDirFromYAML(data []byte, logger log15.Logger) (*GIDFromSubDir, error) {
	var yfgfs yamlForGIDFromSubDir

	err := yaml.Unmarshal(data, &yfgfs)
	if err != nil {
		return nil, err
	}

	if !yfgfs.valid() {
		return nil, errInvalidYAML
	}

	return NewGIDFromSubDir(yfgfs.Prefixes, yfgfs.LookupDir, yfgfs.Lookup, yfgfs.DirectDir, yfgfs.Exceptions, logger)
}
