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
	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/server"
)

// options for this cmd.
var serverBind string
var serverCert string
var serverKey string
var serverDGUT string

// serverCmd represents the server command.
var serverCmd = &cobra.Command{
	Use:   "server",
	Short: "Start the web server",
	Long: `Start the web server.

Starting the web server brings up a REST API that can use the given dgut.db 
(as produced by 'wrstat dgut' during 'wrstat mutli') to answer questions about
where data is on the disks.

Your --bind address should include the port, and for it to work with your
--cert, you probably need to specify it as fqdn:port.

The server must be running for 'wrstat where' calls to succeed.

This command will block forever in the foreground; you can background it with
ctrl-z; bg.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if serverBind == "" {
			die("you must supply --bind")
		}

		if serverDGUT == "" {
			die("you must supply the path to the dgut.db")
		}

		if serverCert == "" {
			die("you must supply --cert")
		}

		if serverKey == "" {
			die("you must supply --key")
		}

		s := server.New()

		err := s.LoadDGUTDB(serverDGUT)
		if err != nil {
			die("failed to load database: %s", err)
		}

		err = s.Start(serverBind, serverCert, serverKey)
		if err != nil {
			die("non-graceful stop: %s", err)
		}
	},
}

func init() {
	RootCmd.AddCommand(serverCmd)

	// flags specific to this sub-command
	serverCmd.Flags().StringVarP(&serverBind, "bind", "b", ":80",
		"address to bind to, eg host:port")
	serverCmd.Flags().StringVarP(&serverDGUT, "db", "d", "",
		"path to dgut.db")
	serverCmd.Flags().StringVarP(&serverCert, "cert", "c", "",
		"path to certificate file")
	serverCmd.Flags().StringVarP(&serverKey, "key", "k", "",
		"path to key file")

	serverCmd.SetHelpFunc(func(command *cobra.Command, strings []string) {
		hideGlobalFlags(serverCmd, command, strings)
	})
}
