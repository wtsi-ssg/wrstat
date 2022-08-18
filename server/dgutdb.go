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

package server

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/fsnotify/fsnotify"
	"github.com/wtsi-ssg/wrstat/dgut"
)

const ErrNoDgutDBDirFound = Error("dgut database directory not found")

// LoadDGUTDBs loads the given dgut.db directories (as produced by one or more
// invocations of dgut.DB.Store()) and adds the /rest/v1/where GET endpoint to
// the REST API. If you call EnableAuth() first, then this endpoint will be
// secured and be available at /rest/v1/auth/where.
//
// The where endpoint can take the dir, splits, groups, users and types
// parameters, which correspond to arguments that dgut.Tree.Where() takes.
func (s *Server) LoadDGUTDBs(paths ...string) error {
	s.treeMutex.Lock()
	defer s.treeMutex.Unlock()

	tree, err := dgut.NewTree(paths...)
	if err != nil {
		return err
	}

	s.tree = tree
	s.dgutPaths = paths

	if s.authGroup == nil {
		s.router.GET(EndPointWhere, s.getWhere)
	} else {
		s.authGroup.GET(wherePath, s.getWhere)
	}

	return nil
}

// EnableDGUTDBReloading will wait for changes to the file at watchPath, then:
// 1. close any previously loaded dgut database files
// 2. find the latest sub-directory in the given directory with the given suffix
// 3. set the dgut.db directory paths to children of 2) and load those
// 4. delete the old dgut.db directory paths to save space
//
// It will only return an error if trying to watch watchPath immediately fails.
// Other errors (eg. reloading or deleting files) will be logged.
func (s *Server) EnableDGUTDBReloading(watchPath, dir, suffix string) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	s.dgutWatcher = watcher

	go s.reactToWatcher(watcher, dir, suffix)

	err = watcher.Add(watchPath)
	if err != nil {
		watcher.Close()
	}

	return err
}

// reactToWatcher loops on watcher events and calls reloadDGUTDBs() in response.
// Call this in a goroutine.
func (s *Server) reactToWatcher(watcher *fsnotify.Watcher, dir, suffix string) {
	for {
		_, ok := <-watcher.Events
		if !ok {
			return
		}

		s.reloadDGUTDBs(dir, suffix)
	}
}

// reloadDGUTDBs closes database files previously loaded during LoadDGUTDBs(),
// looks for the latest subdirectory of the given directory that has the given
// suffix, and loads the children of that as our new dgutPaths.
//
// On success, deletes the previous dgutPaths.
//
// Logs any errors.
func (s *Server) reloadDGUTDBs(dir, suffix string) {
	s.treeMutex.Lock()
	defer s.treeMutex.Unlock()

	if s.tree != nil {
		s.tree.Close()
	}

	oldPaths := s.dgutPaths

	err := s.findNewDgutPaths(dir, suffix)
	if err != nil {
		s.logger.Println("reloading dgut dbs failed:", err)

		return
	}

	s.logger.Printf("reloading dgut dbs from %s\n", s.dgutPaths)

	s.tree, err = dgut.NewTree(s.dgutPaths...)
	if err != nil {
		s.logger.Println("reloading dgut dbs failed:", err)

		return
	}

	s.logger.Println("server ready again after reloading dgut dbs")

	s.deleteDirs(oldPaths)
}

// findNewDgutPaths finds the latest subdirectory of dir that has the given
// suffix, then sets our dgutPaths to the result's children.
func (s *Server) findNewDgutPaths(dir, suffix string) error {
	fis, err := ioutil.ReadDir(dir)
	if err != nil {
		return err
	}

	sort.Slice(fis, func(i, j int) bool {
		return fis[i].ModTime().After(fis[j].ModTime())
	})

	for _, fi := range fis {
		if strings.HasSuffix(fi.Name(), "."+suffix) {
			return s.setNewDgutPaths(filepath.Join(dir, fi.Name()))
		}
	}

	return ErrNoDgutDBDirFound
}

// setNewDgutPaths sets our dgutPaths to the directory contents of the given
// dir.
func (s *Server) setNewDgutPaths(dir string) error {
	des, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	var paths []string

	for _, de := range des {
		if de.IsDir() {
			paths = append(paths, filepath.Join(dir, de.Name()))
		}
	}

	if len(paths) == 0 {
		return ErrNoDgutDBDirFound
	}

	s.dgutPaths = paths

	return nil
}

// deleteDirs deletes the given directories. Logs any errors.
func (s *Server) deleteDirs(dirs []string) {
	for _, dir := range dirs {
		if err := os.RemoveAll(dir); err != nil {
			s.logger.Println("deleting dgut dbs failed:", err)
		}
	}
}
