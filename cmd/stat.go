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

package cmd

import (
	"io/fs"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/wtsi-ssg/wrstat/ch"
	"github.com/wtsi-ssg/wrstat/stat"
	"github.com/wtsi-ssg/wrstat/summary"
)

const reportFrequency = 10 * time.Minute
const statOutputFileSuffix = ".stats"
const statUserGroupSummaryOutputFileSuffix = ".byusergroup"
const statGroupSummaryOutputFileSuffix = ".bygroup"
const statLogOutputFileSuffix = ".log"
const lstatTimeout = 10 * time.Second
const lstatAttempts = 3

var statDebug bool
var statCh string

// statCmd represents the stat command.
var statCmd = &cobra.Command{
	Use:   "stat",
	Short: "Stat paths",
	Long: `Stat paths in a given file.

Given a file containing an absolute file path per line (eg. as produced by
'wrstat walk'), this creates a new file with stats for each of those file paths.
The new file is named after the input file with a ".stats" suffix.

The output file format is 11 tab separated columns with the following contents:
1. Base64 encoded path to the file.
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

It also summarises file count and size information by grouping on
user+group+directory, and stores this summary in another file named after the
input file with a ".byusergroup.gz" suffix. This is 5 tab separated columns with
the following contents (sorted on the first 3 columns):

1. username
2. unix group name
3. directory
4. number of files nested under 3 belonging to both 1 & 2.
5. total file size in bytes of the files in 4.

For example, if user joe using unix group lemur had written 2 10 byte files to
/disk1/dir1, 3 files to /disk1/dir1/dir1a, 1 file to /disk1/dir2, and 1 file to
/disk1/dir1 as unix group fish, then the output would be:

joe	fish	/disk1	1	10
joe	fish	/disk1/dir1	1	10
joe	lemur	/disk1	6	60
joe	lemur	/disk1/dir1	5	50
joe	lemur	/disk1/dir1/dir1a	3	30
joe	lemur	/disk1/dir2	1	10

It also summarises file count and size information by grouping on group+user,
and stores this summary in another file named after the input file with a
".bygroup" suffix. This is 4 tab separated columns with the following contents
(sorted on the first 2 columns):

1. unix group name
2. username
3. number of files belonging to both 1 & 2.
4. total file size in bytes of the files in 3.

If you supply a yaml file to --ch of the following format:
prefixes: ["/disk1", "/disk2/sub", "/disk3"]
lookupDir: subdir_name_of_prefixes_that_contains_subdirs_in_lookup
lookup:
  subdir_of_lookupDir: unix_group_name
directDir: subdir_of_prefixes_with_unix_group_or_exception_subdirs
exceptions:
  subdir_of_directDir: GID 

Then any input filesystem path that has one of those prefixes and contains a sub
directory named lookupDir which further contains a sub directory matching one
of the lookup keys, or named directDir and further containing a sub directory
named after a unix group or one of the exceptions keys, then the following will
be ensured:

1. The GID of the path matches the desired GID. The desired GID is either:
   a) for paths that are nested within a sub directory of a lookupDir, the GID
      corresponding to the unix group in the lookup.
   b) for paths that are nested within a sub directory of a directDir, the GID
      corresponding to the GID in the exceptions, or of the corresponding unix
	  group.
2. If path is a directory, it has setgid applied (group sticky).
3. User execute permission is set if group execute permission was set.
4. Group permissions match user permissions.
5. Both user and group have read and write permissions.

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

	statCmd.Flags().StringVar(&statCh, "ch", "", "YAML file detailing paths to chmod & chown")
	statCmd.Flags().BoolVar(&statDebug, "debug", false, "output Lstat timings")
}

// statPathsInFile does the main work.
func statPathsInFile(inputPath string, yamlPath string, debug bool) {
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

	scanAndStatInput(input, createStatOutputFile(inputPath), yamlPath, debug)
}

// createStatOutputFile creates a file named input.stats.
func createStatOutputFile(input string) *os.File {
	return createOutputFileWithSuffix(input, statOutputFileSuffix)
}

// createOutputFileWithSuffix creates an output file named after prefixPath
// appended with suffix.
func createOutputFileWithSuffix(prefixPath, suffix string) *os.File {
	output, err := os.Create(prefixPath + suffix)
	if err != nil {
		die("failed to create output file: %s", err)
	}

	return output
}

// scanAndStatInput scans through the input, stats each path, and outputs the
// results to the output.
//
// If yamlPath is not empty, also does chmod and chown operations on certain
// paths.
//
// If debug is true, outputs timings for Lstat calls and other operations.
func scanAndStatInput(input, output *os.File, yamlPath string, debug bool) {
	var frequency time.Duration
	if debug {
		frequency = reportFrequency
	}

	statter := stat.WithTimeout(lstatTimeout, lstatAttempts, appLogger)
	p := stat.NewPaths(statter, appLogger, frequency)

	if err := p.AddOperation("file", stat.FileOperation(output)); err != nil {
		die("%s", err)
	}

	postScan, err := addSummaryOperations(input.Name(), p)
	if err != nil {
		die("%s", err)
	}

	if err = addChOperation(yamlPath, p); err != nil {
		die("%s", err)
	}

	if err = p.Scan(input); err != nil {
		die("%s", err)
	}

	if err = postScan(); err != nil {
		die("%s", err)
	}
}

// addSummaryOperations adds summary operations to p. Returns a function that
// should be called after p.Scan.
func addSummaryOperations(input string, p *stat.Paths) (func() error, error) {
	outputUserGroupSummaryData, err := addUserGroupSummaryOperation(input, p)
	if err != nil {
		return nil, err
	}

	outputGroupSummaryData, err := addGroupSummaryOperation(input, p)
	if err != nil {
		return nil, err
	}

	return func() error {
		if err = outputUserGroupSummaryData(); err != nil {
			return err
		}

		return outputGroupSummaryData()
	}, nil
}

// addUserGroupSummaryOperation adds an operation to Paths that collects [user,
// group, directory, count, size] summary information. It returns a function
// that you should call after calling p.Scan(), which outputs the summary data
// to file.
func addUserGroupSummaryOperation(input string, p *stat.Paths) (func() error, error) {
	ug := summary.NewByUserGroup()

	return addSummaryOperator(input, statUserGroupSummaryOutputFileSuffix, "usergroup", p, ug)
}

// outputOperators are types returned by summary.New*().
type outputOperator interface {
	Add(path string, info fs.FileInfo) error
	Output(output *os.File) error
}

// addSummaryOperator adds the operation method of o to p after creating an
// output file with given suffix. Returns function that actually writes to the
// output.
func addSummaryOperator(input, suffix, logName string, p *stat.Paths, o outputOperator) (func() error, error) {
	output := createOutputFileWithSuffix(input, suffix)

	err := p.AddOperation(logName, o.Add)

	return func() error {
		return o.Output(output)
	}, err
}

// addGroupSummaryOperation adds an operation to Paths that collects [group,
// user, count, size] summary information. It returns a function that you should
// call after calling p.Scan(), which outputs the summary data to file.
func addGroupSummaryOperation(input string, p *stat.Paths) (func() error, error) {
	g := summary.NewByGroupUser()

	return addSummaryOperator(input, statGroupSummaryOutputFileSuffix, "group", p, g)
}

// addChOperation adds the chmod&chown operation to the Paths if the yaml file
// has valid contents. No-op if yamlPath is blank.
func addChOperation(yamlPath string, p *stat.Paths) error {
	if yamlPath == "" {
		return nil
	}

	data, err := os.ReadFile(yamlPath)
	if err != nil {
		return err
	}

	from, err := ch.NewGIDFromSubDirFromYAML(data, appLogger)
	if err != nil {
		return err
	}

	c := ch.New(from.PathChecker(), appLogger)

	return p.AddOperation("ch", c.Do)
}
