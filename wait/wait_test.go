/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
 * Author: Ash Holland <ah37@sanger.ac.uk>
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

package wait

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

const modePermUser = 0770

func TestWait(t *testing.T) {
	Convey("Given 2 multi-style output directories", t, func() {
		tdir := t.TempDir()
		sourceDir := filepath.Join(tdir, "source")
		destDir := filepath.Join(tdir, "dest")

		err := os.Mkdir(sourceDir, modePermUser)
		So(err, ShouldBeNil)

		err = os.Mkdir(destDir, modePermUser)
		So(err, ShouldBeNil)

		oldSourceDate := "20240108"
		sourceDate := "20240109"
		destDest := oldSourceDate
		suffix := "basedirs.db"

		oldSourcePath := filepath.Join(sourceDir, oldSourceDate+"_random."+suffix)
		createFile(t, oldSourcePath)
		sourcePath := filepath.Join(sourceDir, sourceDate+"_random."+suffix)
		<-time.After(10 * time.Millisecond)
		createFile(t, sourcePath)
		destPath := filepath.Join(destDir, destDest+"_random."+suffix)
		createFile(t, destPath)
		trickDestPath := filepath.Join(destDir, sourceDate+"_random.trick")
		createFile(t, trickDestPath)

		Convey("ForMatchingPrefixOfLatestSuffix only returns if latest dest has the same prefix as latest source", func() {
			timeLimit := 1 * time.Millisecond
			_, _, err := ForMatchingPrefixOfLatestSuffix(suffix, 8, sourceDir, destDir, timeLimit)
			So(err, ShouldNotBeNil)

			<-time.After(10 * time.Millisecond)
			destPath := filepath.Join(destDir, sourceDate+"_random."+suffix)
			createFile(t, destPath)

			foundSource, foundDest, errw := ForMatchingPrefixOfLatestSuffix(suffix, 8, sourceDir, destDir, timeLimit)
			So(errw, ShouldBeNil)

			So(foundSource, ShouldEqual, sourcePath)
			So(foundDest, ShouldEqual, destPath)

			Convey("and if latest source has the same prefix as latest dest", func() {
				newDate := "20240110"

				<-time.After(10 * time.Millisecond)
				destPath = filepath.Join(destDir, newDate+"_random."+suffix)
				createFile(t, destPath)

				_, _, err := ForMatchingPrefixOfLatestSuffix(suffix, 8, sourceDir, destDir, timeLimit)
				So(err, ShouldNotBeNil)

				sourcePath := filepath.Join(sourceDir, newDate+"_random."+suffix)
				createFile(t, sourcePath)

				foundSource, foundDest, errw := ForMatchingPrefixOfLatestSuffix(suffix, 8, sourceDir, destDir, timeLimit)
				So(errw, ShouldBeNil)

				So(foundSource, ShouldEqual, sourcePath)
				So(foundDest, ShouldEqual, destPath)
			})
		})

		Convey("ForMatchingPrefixOfLatestSuffix waits for latest dest to share prefix with latest source", func() {
			startCh := make(chan struct{})
			resultCh := make(chan error)
			destPath := filepath.Join(destDir, sourceDate+"_random."+suffix)

			runForMatchingPrefixOfLatestSuffix := func(timeLimit time.Duration) {
				<-time.After(10 * time.Millisecond)
				startCh <- struct{}{}

				_, _, err := ForMatchingPrefixOfLatestSuffix(suffix, 8, sourceDir, destDir, timeLimit)
				resultCh <- err
			}

			writeFile := func() {
				<-startCh
				<-time.After(10 * time.Millisecond)
				createFile(t, destPath)
			}

			go runForMatchingPrefixOfLatestSuffix(1 * time.Millisecond)
			writeFile()
			So(<-resultCh, ShouldNotBeNil)

			os.Remove(destPath)

			go runForMatchingPrefixOfLatestSuffix(20 * time.Millisecond)
			writeFile()
			So(<-resultCh, ShouldBeNil)
		})
	})
}

func createFile(t *testing.T, path string) {
	t.Helper()

	file, err := os.Create(path)
	if err != nil {
		t.Fatalf("create file failed: %s", err)
	}

	file.Close()
}
