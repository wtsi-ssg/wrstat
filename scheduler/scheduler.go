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

package scheduler

import (
	"context"
	"os"
	"time"

	"github.com/VertebrateResequencing/wr/jobqueue"
	jqs "github.com/VertebrateResequencing/wr/jobqueue/scheduler"
	"github.com/inconshreveable/log15"
	"github.com/rs/xid"
	"github.com/wtsi-ssg/wr/clog"
)

type Error string

func (e Error) Error() string { return string(e) }

const errDupJobs = Error("some of the added jobs were duplicates")

// some consts for the jobs returned by NewJob().
const jobRetries uint8 = 30
const reqRAM = 100
const reqTime = 10 * time.Second
const reqCores = 1
const reqDisk = 1

// Scheduler can be used to schedule commands to be executed by adding them to
// wr's queue.
type Scheduler struct {
	cwd   string
	exe   string
	jq    *jobqueue.Client
	sudo  bool
	queue string
}

// New returns a Scheduler that is connected to wr manager using the given
// deployment, timeout and logger. If sudo is true, NewJob() will prefix 'sudo'
// to commands. Added jobs will have the given cwd, which matters. If cwd is
// blank, the current working dir is used. If queue is not blank, that queue
// will be used during NewJob().
func New(deployment, cwd, queue string, timeout time.Duration, logger log15.Logger,
	sudo bool) (*Scheduler, error) {
	cwd, err := pickCWD(cwd)
	if err != nil {
		return nil, err
	}

	jq, err := jobqueue.ConnectUsingConfig(clog.ContextWithLogHandler(context.Background(),
		logger.GetHandler()), deployment, timeout)
	if err != nil {
		return nil, err
	}

	exe, err := os.Executable()

	return &Scheduler{
		cwd:   cwd,
		exe:   exe,
		queue: queue,
		jq:    jq,
		sudo:  sudo,
	}, err
}

// pickCWD checks the given directory exists, returns an error. If the given
// dir is blank, returns the current working directory.
func pickCWD(cwd string) (string, error) {
	if cwd == "" {
		return os.Getwd()
	}

	_, err := os.Stat(cwd)

	return cwd, err
}

// Executable is a convenience function that returns the same as
// os.Executable(), but without the error.
func (s *Scheduler) Executable() string {
	return s.exe
}

// DefaultRequirements returns a minimal set of requirments, which is what
// NewJob() will use by default.
func DefaultRequirements() *jqs.Requirements {
	return &jqs.Requirements{
		RAM:   reqRAM,
		Time:  reqTime,
		Cores: reqCores,
		Disk:  reqDisk,
	}
}

// NewJob is a convenience function for creating Jobs. It sets the job's Cwd
// to the current working directory, sets CwdMatters to true, applies the given
// Requirements, and sets Retries to 3.
//
// If this Scheduler had been made with sudo: true, cmd will be prefixed with
// 'sudo '.
//
// THe supplied depGroup and dep can be blank to not set DepGroups and
// Dependencies.
//
// If req is supplied, sets the job override to 1. Otherwise, req will default
// to a minimal set of requirments, and override will be 0. If this Scheduler
// had been made with a queue override, the requirements will be altered to add
// that queue.
func (s *Scheduler) NewJob(cmd, repGroup, reqGroup, depGroup, dep string, req *jqs.Requirements) *jobqueue.Job {
	if s.sudo {
		cmd = "sudo " + cmd
	}

	req, override := s.determineOverrideAndReq(req)

	return &jobqueue.Job{
		Cmd:          cmd,
		Cwd:          s.cwd,
		CwdMatters:   true,
		RepGroup:     repGroup,
		ReqGroup:     reqGroup,
		Requirements: req,
		DepGroups:    createDepGroups(depGroup),
		Dependencies: createDependencies(dep),
		Retries:      jobRetries,
		Override:     override,
	}
}

// createDepGroups returns the given depGroup inside a string slice, unless
// blank, in which case returns nil slice.
func createDepGroups(depGroup string) []string {
	var depGroups []string
	if depGroup != "" {
		depGroups = []string{depGroup}
	}

	return depGroups
}

// createDependencies returns the given dep as a Dependencies if not blank,
// otherwise nil.
func createDependencies(dep string) jobqueue.Dependencies {
	var dependencies jobqueue.Dependencies
	if dep != "" {
		dependencies = jobqueue.Dependencies{{DepGroup: dep}}
	}

	return dependencies
}

// determineOverrideAndReq returns the given req and an override of 1 if req is
// not nil, otherwise returns a default req and override of 0.
func (s *Scheduler) determineOverrideAndReq(req *jqs.Requirements) (*jqs.Requirements, uint8) {
	override := 1

	if req == nil {
		req = DefaultRequirements()
		override = 0
	}

	if s.queue != "" {
		other := req.Other
		if other == nil {
			other = make(map[string]string)
		}

		other["scheduler_queue"] = s.queue
		req.Other = other
	}

	return req, uint8(override)
}

// SubmitJobs adds the given jobs to wr's queue, passing through current
// environment variables.
//
// Previously added identical jobs that have since been archived will get added
// again.
//
// If any duplicate jobs were added, an error will be returned.
func (s *Scheduler) SubmitJobs(jobs []*jobqueue.Job) error {
	inserts, _, err := s.jq.Add(jobs, os.Environ(), false)
	if err != nil {
		return err
	}

	if inserts != len(jobs) {
		return errDupJobs
	}

	return nil
}

// Disconnect disconnects from the manager. You should defer this after New().
func (s *Scheduler) Disconnect() error {
	return s.jq.Disconnect()
}

// UniqueString returns a unique string that could be useful for supplying as
// depGroup values to NewJob() etc. The length is always 20 characters.
func UniqueString() string {
	return xid.New().String()
}
