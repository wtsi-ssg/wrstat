/*******************************************************************************
 * Copyright (c) 2022 Genome Research Ltd.
 *
 * Authors:
 *	- Sendu Bala <sb10@sanger.ac.uk>
 *	- Michael Grace <mg38@sanger.ac.uk>
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
	"bufio"
	"bytes"
	"log"
	"os/exec"
	"os/user"
	"regexp"
	"strings"
)

const sudoLRootPrivsUser = "ALL"

var sudoLMayRunRegexp = regexp.MustCompile(`\(\s*(\S+)\s*\)\s*ALL`)

const sudoLMayRunRegexpMatches = 2

// GetUsersUIDs returns the uid for the given username, and also the uids of any
// other users the user can sudo as. If the user can sudo as root, returns nil.
func GetUsersUIDs(username string) ([]string, error) {
	u, err := user.Lookup(username)
	if err != nil {
		return nil, err
	}

	uids, root := getSudoUsers(u.Username)

	if root {
		return nil, nil
	}

	uids = append(uids, u.Uid)

	return uids, nil
}

// getSudoUsers tries to find out what other users the given user can sudo as.
// Returns those UIDs, if any, and false. If the user can sudo as root, returns
// nil and true. Errors encountered when trying to work this out are logged but
// otherwise ignored, so that the user can still access info about their own
// files.
func getSudoUsers(username string) (uids []string, rootPower bool) {
	out, err := getSudoLOutput(username)
	if err != nil {
		return uids, false
	}

	scanner := bufio.NewScanner(bytes.NewReader(out))

	return parseSudoLOutput(scanner)
}

// getSudoLOutput runs `sudo -l -U usernamer` and returns the output, logging
// any error.
func getSudoLOutput(username string) ([]byte, error) {
	cmd := exec.Command("sudo", "-l", "-U", username)

	out, err := cmd.CombinedOutput()
	if err != nil {
		log.Printf("failed to check sudo ability for %s: %s\n", username, err)

		return nil, err
	}

	return out, nil
}

// parseSudoLOutput takes a scanner of the output of getSudoLOutput() and
// returns the UIDs that the user can run ALL commands for, ie. can sudo as.
// Returns nil, true if user can sudo as root.
func parseSudoLOutput(scanner *bufio.Scanner) (uids []string, rootPower bool) {
	var check bool

	for scanner.Scan() {
		if strings.Contains(scanner.Text(), "may run the following commands") {
			check = true

			continue
		}

		if !check {
			continue
		}

		uid := getUIDFromSudoLOutput(scanner.Text())

		if uid == "ALL" {
			return nil, true
		}

		if uid != "" {
			uids = append(uids, uid)
		}
	}

	return uids, false
}

// getUIDFromSudoLOutput parses the username from the supplied line of output
// from getSudoLOutput(). It converts the username to a UID and returns it. If
// it returns "ALL", it means the user can sudo as root.
func getUIDFromSudoLOutput(line string) string {
	matches := sudoLMayRunRegexp.FindStringSubmatch(line)

	if len(matches) != sudoLMayRunRegexpMatches {
		return ""
	}

	if matches[1] == sudoLRootPrivsUser {
		return sudoLRootPrivsUser
	}

	u, err := user.Lookup(matches[1])
	if err != nil {
		return ""
	}

	if u.Uid == "0" {
		return sudoLRootPrivsUser
	}

	return u.Uid
}

var whiteListGIDs = map[string]struct{}{ // nolint:golint,gochecknoglobals // could this be in a config file
	"1313":  {},
	"1818":  {},
	"15306": {},
	"1662":  {},
	"15394": {},
}

// WhiteLister is currently hard-coded to say that membership of certain
// gids means users should be treated like root.
func WhiteLister(gid string) bool {
	_, ok := whiteListGIDs[gid]

	return ok
}
