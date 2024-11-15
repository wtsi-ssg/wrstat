/*******************************************************************************
 * Copyright (c) 2021-2022 Genome Research Ltd.
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
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/v5/ch"
	"github.com/wtsi-ssg/wrstat/v5/stat"
)

const (
	reportFrequency         = 10 * time.Minute
	statOutputFileSuffix    = ".stats"
	statLogOutputFileSuffix = ".log"
	lstatTimeout            = 10 * time.Second
	lstatAttempts           = 3
	lstatConsecutiveFails   = 10
	scanTimeout             = 2 * time.Hour
)

var (
	statDebug bool
	statCh    string
)

// statCmd represents the stat command.
var statCmd = &cobra.Command{
	Use:   "stat",
	Short: "Stat paths",
	Long: `Stat paths in a given file.

Given a file containing a quoted absolute file path per line (eg. as produced
by 'wrstat walk'), this creates a new file with stats for each of those file
paths. The new file is named after the input file with a ".stats" suffix.

The output file format is 11 tab separated columns with the following contents:
1. Quoted path to the file.
2. File size in bytes. If this is greater than the number of bytes in blocks
   allocated, this will be the number of bytes in allocated blocks. (This is to
   account for files with holes in them; as a byproduct, symbolic links will
   be reported as 0 size.)
3. UID.
4. GID.
5. Atime (time of most recent access expressed in seconds).
6. Mtime (time of most recent content modification expressed in seconds.)
7. Ctime (on unix, the time of most recent metadata change in seconds).
8. Filetype:
   'f': regular file
   'l': symbolic link
   's': socket
   'b': block special device file
   'c': character special device file
   'F': FIFO (named pipe)
   'X': anything else
9. Inode number (on unix).
10. Number of hard links.
11. Identifier of the device on which this file resides.

If you supply a tsv file to --ch with the following columns:
directory user group fileperms dirperms
[where *perms format is rwxrwxrwx for user,group,other, where - means remove the
permission, * means leave it unchanged, and one of [rwx] means set it. s for the
group x would enable setting group sticky bit. s implies x. Using ^ in at
least 2 equivalent places means "set all if any set". ie. '**^**^***' would mean
"change nothing, except if execute is set on user or group, set it on both".
user and group can be unix username or unix group name. * means don't set it.
Use ^ to mean copy from the directory.
The file can have blank lines and comment lines that begin with #, which will be
ignored.]
Then any input filesystem path in one of those directories will have its
permissions and ownership changed if needed.

(Any changes caused by this will not be reflected in the output file, since
the chmod and chown operations happen after path's stats are retrieved.)

Finally, log messages (including things like warnings and errors while working
on the above) are stored in another file named after the input file with a
".log" suffix.
`,
	Run: func(cmd *cobra.Command, args []string) {
		if len(args) != 1 {
			die("exactly 1 input file should be provided")
		}

		logToFile(args[0] + statLogOutputFileSuffix)

		statPathsInFile(args[0], statCh, statDebug)
	},
}

func init() {
	RootCmd.AddCommand(statCmd)

	statCmd.Flags().StringVar(&statCh, "ch", "", "tsv file detailing paths to chmod & chown")
	statCmd.Flags().BoolVar(&statDebug, "debug", false, "output Lstat timings")
}

// statPathsInFile does the main work.
func statPathsInFile(inputPath string, tsvPath string, debug bool) {
	input, err := os.Open(inputPath)
	if err != nil {
		die("failed to open input file: %s", err)
	}

	defer func() {
		err = input.Close()
		if err != nil {
			warn("failed to close input file: %s", err)
		}
	}()

	scanAndStatInput(input, createStatOutputFile(inputPath), tsvPath, debug)
}

// createStatOutputFile creates a file named input.stats.
func createStatOutputFile(input string) *os.File {
	return createOutputFileWithSuffix(input, statOutputFileSuffix)
}

// createOutputFileWithSuffix creates an output file named after prefixPath
// appended with suffix.
func createOutputFileWithSuffix(prefixPath, suffix string) *os.File {
	fname := prefixPath + suffix

	hostname, err := os.Hostname()
	if err != nil {
		die("failed to get hostname: %s", err)
	}

	output, err := os.Create(fmt.Sprintf("%s.%s.%d", fname, hostname, os.Getpid()))
	if err != nil {
		die("failed to create output file: %s", err)
	}

	os.Remove(fname)

	if err = os.Symlink(output.Name(), fname); err != nil {
		die("failed to create symlink: %s", err)
	}

	return output
}

// scanAndStatInput scans through the input, stats each path, and outputs the
// results to the output.
//
// If tsvPath is not empty, also does chmod and chown operations on certain
// paths.
//
// If debug is true, outputs timings for Lstat calls and other operations.
func scanAndStatInput(input, output *os.File, tsvPath string, debug bool) {
	var frequency time.Duration
	if debug {
		frequency = reportFrequency
	}

	statter := stat.WithTimeout(lstatTimeout, lstatAttempts, lstatConsecutiveFails, appLogger)
	pConfig := stat.PathsConfig{Logger: appLogger, ReportFrequency: frequency, ScanTimeout: scanTimeout}
	p := stat.NewPaths(statter, pConfig)

	if err := p.AddOperation("file", stat.FileOperation(output)); err != nil {
		die("%s", err)
	}

	if err := addChOperation(tsvPath, p); err != nil {
		die("%s", err)
	}

	if err := p.Scan(input); err != nil {
		die("%s", err)
	}
}

// addChOperation adds the chmod&chown operation to the Paths if the tsv file
// has valid contents. No-op if tsvPath is blank.
func addChOperation(tsvPath string, p *stat.Paths) error {
	if tsvPath == "" {
		return nil
	}

	f, err := os.Open(tsvPath)
	if err != nil {
		return err
	}

	defer f.Close()

	rs, err := ch.NewRulesStore().FromTSV(ch.NewTSVReader(f))
	if err != nil {
		return err
	}

	c := ch.New(rs, appLogger)

	return p.AddOperation("ch", c.Do)
}
