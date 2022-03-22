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

package cmd

import (
	"bufio"
	"bytes"
	"fmt"
	"log/syslog"
	"os/exec"
	"os/user"
	"regexp"
	"strings"

	ldap "github.com/go-ldap/ldap/v3"
	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/server"
)

const sudoLRootPrivsUser = "ALL"

var sudoLMayRunRegexp = regexp.MustCompile(`\(\s*(\S+)\s*\)\s*ALL`)

const sudoLMayRunRegexpMatches = 2

// options for this cmd.
var serverBind string
var serverCert string
var serverKey string
var serverLDAPFQDN string
var serverLDAPBindDN string
var syslogWriter *syslog.Writer

// serverCmd represents the server command.
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the web server",
	Long: `Start the web server.

Starting the web server brings up a REST API that can use the given dgut.db
files (as produced by 'wrstat dgut' during 'wrstat mutli') to answer questions
about where data is on the disks. (Provide the list as unamed args.)

Your --bind address should include the port, and for it to work with your
--cert, you probably need to specify it as fqdn:port.

The server authenticates users using LDAP. You must provide the FQDN for your
LDAP server, eg. --ldap_server ldap.example.com, and the bind DN that you would
supply to eg. 'ldapwhoami -D' to test user credentials, replacing the username
part with '%s', eg. --ldap_dn 'uid=%s,ou=people,dc=example,dc=com'.

The server will log all messages (of any severity) to syslog at the INFO level,
except for non-graceful stops of the server, which are sent at the CRIT level or
include 'panic' in the message. The messages are tagged 'wrstat-server', and you
might want to filter away 'STATUS=200' to find problems.

The server must be running for 'wrstat where' calls to succeed.

This command will block forever in the foreground; you can background it with
ctrl-z; bg.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) == 0 {
			die("you must supply the path(s) to your dgut.db file(s)")
		}

		if serverBind == "" {
			die("you must supply --bind")
		}

		if serverCert == "" {
			die("you must supply --cert")
		}

		if serverKey == "" {
			die("you must supply --key")
		}

		if serverLDAPFQDN == "" {
			die("you must supply --ldap_server")
		}

		if serverLDAPBindDN == "" {
			die("you must supply --ldap_dn")
		}

		var err error
		syslogWriter, err = syslog.New(syslog.LOG_INFO|syslog.LOG_DAEMON, "wrstat-server")
		if err != nil {
			die("failed to connect to syslog: %s", err)
		}

		s := server.New(syslogWriter)

		err = s.EnableAuth(serverCert, serverKey, authenticate)
		if err != nil {
			msg := fmt.Sprintf("failed to enable authentication: %s", err)
			syslogWriter.Crit(msg) //nolint:errcheck
			die(msg)
		}

		err = s.LoadDGUTDBs(args...)
		if err != nil {
			msg := fmt.Sprintf("failed to load database: %s", err)
			syslogWriter.Crit(msg) //nolint:errcheck
			die(msg)
		}

		err = s.Start(serverBind, serverCert, serverKey)
		if err != nil {
			msg := fmt.Sprintf("non-graceful stop: %s", err)
			syslogWriter.Crit(msg) //nolint:errcheck
			die(msg)
		}
	},
}

func init() {
	RootCmd.AddCommand(serverCmd)

	// flags specific to this sub-command
	serverCmd.Flags().StringVarP(&serverBind, "bind", "b", ":80",
		"address to bind to, eg host:port")
	serverCmd.Flags().StringVarP(&serverCert, "cert", "c", "",
		"path to certificate file")
	serverCmd.Flags().StringVarP(&serverKey, "key", "k", "",
		"path to key file")
	serverCmd.Flags().StringVarP(&serverLDAPFQDN, "ldap_server", "s", "",
		"fqdn of your ldap server")
	serverCmd.Flags().StringVarP(&serverLDAPBindDN, "ldap_dn", "l", "",
		"ldap bind dn, with username replaced with %s")

	serverCmd.SetHelpFunc(func(command *cobra.Command, strings []string) {
		hideGlobalFlags(serverCmd, command, strings)
	})
}

// authenticate verifies the user's password against LDAP, and if correct
// returns true alog with all the uids they can sudo as, and all the gids all
// those users belong to.
func authenticate(username, password string) (bool, []string, []string) {
	err := checkLDAPPassword(username, password)
	if err != nil {
		return false, nil, nil
	}

	uids, err := getUsersUIDs(username)
	if err != nil {
		syslogWriter.Warning(fmt.Sprintf("failed to get UIDs for %s: %s", username, err)) //nolint:errcheck

		return false, nil, nil
	}

	gids, err := getGIDsForUsers(uids)
	if err != nil {
		syslogWriter.Warning(fmt.Sprintf("failed to get GIDs for %s: %s", username, err)) //nolint:errcheck

		return false, nil, nil
	}

	return true, uids, gids
}

// checkLDAPPassword checks with LDAP if the given password is valid for the
// given username. Returns nil if valid, error otherwise.
func checkLDAPPassword(username, password string) error {
	l, err := ldap.DialURL(fmt.Sprintf("ldaps://%s:636", serverLDAPFQDN))
	if err != nil {
		return err
	}

	return l.Bind(fmt.Sprintf(serverLDAPBindDN, username), password)
}

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
		syslogWriter.Warning(fmt.Sprintf("failed to check sudo ability for %s: %s", username, err)) //nolint:errcheck

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

// getGIDsForUser returns the group IDs that the given user belongs to.
func getGIDsForUser(uid string) ([]string, error) {
	u, err := user.LookupId(uid)
	if err != nil {
		return nil, err
	}

	return u.GroupIds()
}
