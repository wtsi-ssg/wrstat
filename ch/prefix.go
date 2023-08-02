/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
 *
 * Authors: Michael Woolnough <mw31@sanger.ac.uk>
 *          Sendu Bala <sb10@sanger.ac.uk>
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

package ch

import (
	"path/filepath"
	"strings"
)

// pathPrefixTree stores information about file paths that let you find the longest
// directory containing a new path.
type pathPrefixTree struct {
	children map[string]*pathPrefixTree
	leaf     string
}

func newPathPrefixTree() *pathPrefixTree {
	return &pathPrefixTree{
		children: make(map[string]*pathPrefixTree),
	}
}

// addDirectory adds a new directory to the tree.
func (p *pathPrefixTree) addDirectory(directory string) {
	for dir, rest := splitPath(directory[1:]); dir != ""; dir, rest = splitPath(rest) {
		p = p.child(dir)
	}

	p.leaf = directory
}

func splitPath(path string) (string, string) {
	pos := strings.IndexByte(path, filepath.Separator)

	if pos == -1 {
		return path, ""
	}

	return path[:pos], path[pos+1:]
}

func (p *pathPrefixTree) child(directory string) *pathPrefixTree {
	tree, exists := p.children[directory]
	if !exists {
		tree = newPathPrefixTree()
		p.children[directory] = tree
	}

	return tree
}

// longestPrefix finds the longest directory in the tree that is a prefix of
// the given path.
func (p *pathPrefixTree) longestPrefix(path string) (string, bool) {
	for dir, rest := splitPath(path[1:]); dir != ""; dir, rest = splitPath(rest) {
		tree, found := p.children[dir]
		if !found {
			if p.leaf != "" {
				return p.leaf, true
			}

			break
		}

		p = tree
	}

	return "", false
}
