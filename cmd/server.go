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
	"io"
	"log/syslog"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	ldap "github.com/go-ldap/ldap/v3"
	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/server"
)

const sudoLRootPrivsUser = "ALL"
const logFilePerms = 0644

var sudoLMayRunRegexp = regexp.MustCompile(`\(\s*(\S+)\s*\)\s*ALL`)

const sudoLMayRunRegexpMatches = 2

// options for this cmd.
var serverLogPath string
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

Starting the web server brings up a web interface and REST API that can use the
dgut.dbs directory inside the given 'wrstat multi' output directory to answer
questions about where data is on the disks. (Provide your 'wrstat multi -f'
argument as an unamed argument to this command.)

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
If --logfile is supplied, logs to that file instaed of syslog.

The server must be running for 'wrstat where' calls to succeed.

This command will block forever in the foreground; you can background it with
ctrl-z; bg. Or better yet, use the daemonize program to daemonize this.

It will monitor a file called ".dgut.dbs.updated" in the given directory and
attempt to reload the databases when the file is updated by another run of
'wrstat multi' with the same output directory. After reloading, will delete the
dgut.dbs.old directory containing the previous run's database files.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			die("you must supply the path to your 'wrstat multi -f' output directory")
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

		s := server.New(serverLogger(serverLogPath))

		err := s.EnableAuth(serverCert, serverKey, authenticate)
		if err != nil {
			msg := fmt.Sprintf("failed to enable authentication: %s", err)
			syslogWriter.Crit(msg) //nolint:errcheck
			die(msg)
		}

		info("opening databases, please wait...")
		err = s.LoadDGUTDBs(dgutDBPaths(args[0])...)
		if err != nil {
			msg := fmt.Sprintf("failed to load database: %s", err)
			syslogWriter.Crit(msg) //nolint:errcheck
			die(msg)
		}

		sentinel := filepath.Join(args[0], dgutDBsSentinelBasename)

		err = s.EnableDGUTDBReloading(sentinel, args[0], dgutDBsSuffix)
		if err != nil {
			msg := fmt.Sprintf("failed to set up database reloading: %s", err)
			syslogWriter.Crit(msg) //nolint:errcheck
			die(msg)
		}

		err = s.AddTreePage()
		if err != nil {
			msg := fmt.Sprintf("failed to add tree page: %s", err)
			syslogWriter.Crit(msg) //nolint:errcheck
			die(msg)
		}

		defer s.Stop()

		sayStarted()

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
	serverCmd.Flags().StringVar(&serverLogPath, "logfile", "",
		"log to this file instead of syslog")

	serverCmd.SetHelpFunc(func(command *cobra.Command, strings []string) {
		hideGlobalFlags(serverCmd, command, strings)
	})
}

// serverLogger returns an io.Writer for the server to log to. The writer will
// append to the given file. If the given path is blank, writes to syslog
// instead.
func serverLogger(path string) io.Writer {
	if path == "" {
		s, err := syslog.New(syslog.LOG_INFO|syslog.LOG_DAEMON, "wrstat-server")
		if err != nil {
			die("failed to connect to syslog: %s", err)
		}

		return s
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, logFilePerms)
	if err != nil {
		die("failed to open log file: %s", err)
	}

	return f
}

// authenticate verifies the user's password against LDAP, and if correct
// returns true alog with all the uids they can sudo as.
func authenticate(username, password string) (bool, []string) {
	err := checkLDAPPassword(username, password)
	if err != nil {
		return false, nil
	}

	uids, err := getUsersUIDs(username)
	if err != nil {
		syslogWriter.Warning(fmt.Sprintf("failed to get UIDs for %s: %s", username, err)) //nolint:errcheck

		return false, nil
	}

	return true, uids
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

// dgutDBPaths returns the dgut db directories that 'wrstat tidy' creates in the
// given output directory.
func dgutDBPaths(dir string) []string {
	paths, err := filepath.Glob(fmt.Sprintf("%s/*.%s/*", dir, dgutDBsSuffix))
	if err != nil || len(paths) == 0 {
		die("failed to find dgut database directories based on [%s/*.%s/*] (err: %s)", dir, dgutDBsSuffix, err)
	}

	return paths
}

// sayStarted logs to console that the server stated. It does this a second
// after being calling in a goroutine, when we can assume the server has
// actually started; if it failed, we expect it to do so in less than a second
// and exit.
func sayStarted() {
	<-time.After(1 * time.Second)

	info("server started")
}
