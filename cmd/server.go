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

package cmd

import (
	"fmt"
	"log/syslog"
	"path/filepath"
	"regexp"
	"time"

	ldap "github.com/go-ldap/ldap/v3"
	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/server"
)

const sudoLRootPrivsUser = "ALL"

var sudoLMayRunRegexp = regexp.MustCompile(`\(\s*(\S+)\s*\)\s*ALL`)

const sudoLMayRunRegexpMatches = 2

// options for this cmd.
var serverBind string
var userAddress string
var serverCert string
var serverKey string
var serverLDAPFQDN string
var serverLDAPBindDN string
var syslogWriter *syslog.Writer
var oktaOAuthIssuer string
var oktaOAuthClientID string
var oktaOAuthClientSecret string

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

		oktaCLIFlagCounter := 0
		if oktaOAuthIssuer != "" {
			oktaCLIFlagCounter++
		}

		if oktaOAuthClientID != "" {
			oktaCLIFlagCounter++
		}

		if oktaOAuthClientSecret != "" {
			oktaCLIFlagCounter++
		}

		if oktaCLIFlagCounter != 0 && oktaCLIFlagCounter != 3 {
			// if part of the Okta info is specified, it all needs to
			// be specified
			die("to use Okta login, you must specify --okta_issuer, --okta_id and --okta_secret")
		}

		var err error
		syslogWriter, err = syslog.New(syslog.LOG_INFO|syslog.LOG_DAEMON, "wrstat-server")
		if err != nil {
			die("failed to connect to syslog: %s", err)
		}

		s := server.New(syslogWriter)
		s.Address = userAddress

		err = s.EnableAuth(serverCert, serverKey, authenticate)
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
		oldDB := filepath.Join(args[0], dgutDBsOldBasename)

		err = s.EnableDGUTDBReloading(sentinel, oldDB)
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

		s.AddOIDCRoutes(oktaOAuthIssuer, oktaOAuthClientID, oktaOAuthClientSecret)

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
	serverCmd.Flags().StringVarP(&userAddress, "user_address", "", serverBind,
		"the address the user will be visiting (and is used for the OAuth callbacks)")
	serverCmd.Flags().StringVarP(&serverCert, "cert", "c", "",
		"path to certificate file")
	serverCmd.Flags().StringVarP(&serverKey, "key", "k", "",
		"path to key file")
	serverCmd.Flags().StringVarP(&serverLDAPFQDN, "ldap_server", "s", "",
		"fqdn of your ldap server")
	serverCmd.Flags().StringVarP(&serverLDAPBindDN, "ldap_dn", "l", "",
		"ldap bind dn, with username replaced with %s")
	serverCmd.Flags().StringVarP(&oktaOAuthIssuer, "okta_issuer", "", "",
		"URL for Okta Oauth")
	serverCmd.Flags().StringVarP(&oktaOAuthClientID, "okta_id", "", "",
		"Okta Client ID")
	serverCmd.Flags().StringVarP(&oktaOAuthClientSecret, "okta_secret", "", "",
		"Okta Client Secret")

	serverCmd.SetHelpFunc(func(command *cobra.Command, strings []string) {
		hideGlobalFlags(serverCmd, command, strings)
	})
}

// authenticate verifies the user's password against LDAP, and if correct
// returns true alog with all the uids they can sudo as, and all the gids all
// those users belong to.
func authenticate(username, password string) (bool, []string) {
	err := checkLDAPPassword(username, password)
	if err != nil {
		return false, nil
	}

	uids, err := server.GetUsersUIDs(username)
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

// dgutDBPaths returns the dgut db directories that 'wrstat tidy' creates in the
// given output directory.
func dgutDBPaths(dir string) []string {
	paths, err := filepath.Glob(fmt.Sprintf("%s/%s/*", dir, dgutDBsBasename))
	if err != nil || len(paths) == 0 {
		die("failed to find dgut database directories based on [%s/%s/*] (err: %s)", dir, dgutDBsBasename, err)
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
