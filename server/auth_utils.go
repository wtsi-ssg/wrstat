package server

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os/exec"
	"os/user"
	"regexp"
	"strings"
)

const sudoLRootPrivsUser = "ALL"

var sudoLMayRunRegexp = regexp.MustCompile(`\(\s*(\S+)\s*\)\s*ALL`)

const sudoLMayRunRegexpMatches = 2

// getUsersUIDs returns the uid for the given username, and also the uids of any
// other users the user can sudo as. If the user can sudo as root, returns nil.
func getUsersUIDs(username string) ([]string, error) {
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
		log.Println(fmt.Sprintf("failed to check sudo ability for %s: %s", username, err)) //nolint:errcheck

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

// getGIDsForUser returns the group IDs that the given user belongs to.
func getGIDsForUser(uid string) ([]string, error) {
	u, err := user.LookupId(uid)
	if err != nil {
		return nil, err
	}

	return u.GroupIds()
}

// getGIDsForUsers returns the group ids the given user ids belong to. If no
// users are supplied, returns nil.
func getGIDsForUsers(uids []string) ([]string, error) {
	if uids == nil {
		return nil, nil
	}

	gidMap := make(map[string]bool)

	for _, uid := range uids {
		theseGids, err := getGIDsForUser(uid)
		if err != nil {
			return nil, err
		}

		for _, gid := range theseGids {
			gidMap[gid] = true
		}
	}

	gids := make([]string, len(gidMap))
	i := 0

	for gid := range gidMap {
		gids[i] = gid
		i++
	}

	return gids, nil
}
