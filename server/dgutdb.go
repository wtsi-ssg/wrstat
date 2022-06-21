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
	"os"

	"github.com/fsnotify/fsnotify"
	"github.com/wtsi-ssg/wrstat/dgut"
)

// LoadDGUTDBs loads the given dgut.db directories (as produced by one or more
// invocations of dgut.DB.Store()) and adds the /rest/v1/where GET endpoint to
// the REST API. If you call EnableAuth() first, then this endpoint will be
// secured and be available at /rest/v1/auth/where.
//
// The where endpoint can take the dir, splits, groups, users and types
// parameters, which correspond to arguments that dgut.Tree.Where() takes.
func (s *Server) LoadDGUTDBs(paths ...string) error {
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

// EnableDGUTDBReloading will wait for changes to the file at watchPath, then
// close any previously loaded dgut database files before reloading the paths
// you previously called LoadDGUTDBs on. It will then delete the oldDir path.
//
// The idea would be to LoadDGUTDBs(paths_in_dirX), then when you want to use
// new database files, mv dirX to oldDir, put new database files in
// paths_in_dirX and touch watchPath. The new database files will get used, and
// the old ones will be deleted.
//
// It will only return an error if trying to watch watchPath immediately fails.
// Other errors (eg. reloading or deleting oldDir) will be logged.
func (s *Server) EnableDGUTDBReloading(watchPath, oldDir string) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	s.dgutWatcher = watcher

	go s.reactToWatcher(oldDir)

	err = watcher.Add(watchPath)
	if err != nil {
		watcher.Close()
	}

	return err
}

// reactToWatcher loops on watcher events and calls reloadDGUTDBs() in response.
// Call this in a goroutine.
func (s *Server) reactToWatcher(oldDir string) {
	for {
		_, ok := <-s.dgutWatcher.Events
		if !ok {
			return
		}

		s.reloadDGUTDBs(oldDir)
	}
}

// reloadDGUTDBs closes database files previously loaded during LoadDGUTDBs(),
// then reloads (presumably new) ones at the same paths as the prior load.
//
// On success, deletes the given directory.
//
// Logs any errors.
func (s *Server) reloadDGUTDBs(oldDir string) {
	var err error

	s.tree.Close()

	s.tree, err = dgut.NewTree(s.dgutPaths...)
	if err != nil {
		s.logger.Println("reloading dgut dbs failed: ", err)

		return
	}

	s.deleteDir(oldDir)
}

// deleteDir deletes the given directory. Logs any errors.
func (s *Server) deleteDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		s.logger.Println("deleting dgut dbs failed: ", err)
	}
}
