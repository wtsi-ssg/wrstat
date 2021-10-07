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

const dupJobsErr = Error("some of the added jobs were duplicates")

// some consts for the jobs returned by NewJob().
const jobRetries uint8 = 3
const reqRAM = 50
const reqTime = 2 * time.Second
const reqCores = 1
const reqDisk = 1

// Scheduler can be used to schedule commands to be executed by adding them to
// wr's queue.
type Scheduler struct {
	cwd          string
	exe          string
	requirements *jqs.Requirements
	jq           *jobqueue.Client
	sudo         bool
}

// New returns a Scheduler that is connected to wr manager using the given
// deployment, timeout and logger. If sudo is true, NewJob() will prefix 'sudo'
// to commands.
func New(deployment string, timeout time.Duration, logger log15.Logger, sudo bool) (*Scheduler, error) {
	wd, err := os.Getwd()
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
		cwd: wd,
		exe: exe,
		requirements: &jqs.Requirements{
			RAM:   reqRAM,
			Time:  reqTime,
			Cores: reqCores,
			Disk:  reqDisk,
		},
		jq:   jq,
		sudo: sudo,
	}, err
}

// Executable is a convenience function that returns the same as
// os.Executable(), but without the error.
func (s *Scheduler) Executable() string {
	return s.exe
}

// NewJob is a convenience function for creating Jobs. It sets the job's Cwd
// to the current working directory, sets CwdMatters to true, applies a minimal
// Requirements, and sets Retries to 3.
//
// If this Scheduler had been made with sudo: true, cmd will be prefixed with
// 'sudo '.
//
// THe supplied depGroup and dep can be blank to not set DepGroups and
// Dependencies.
func (s *Scheduler) NewJob(cmd, rep, req, depGroup, dep string) *jobqueue.Job {
	var depGroups []string
	if depGroup != "" {
		depGroups = []string{depGroup}
	}

	var dependencies jobqueue.Dependencies
	if dep != "" {
		dependencies = jobqueue.Dependencies{{DepGroup: dep}}
	}

	if s.sudo {
		cmd = "sudo " + cmd
	}

	return &jobqueue.Job{
		Cmd:          cmd,
		Cwd:          s.cwd,
		CwdMatters:   true,
		RepGroup:     rep,
		ReqGroup:     req,
		Requirements: s.requirements,
		DepGroups:    depGroups,
		Dependencies: dependencies,
		Retries:      jobRetries,
	}
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
		return dupJobsErr
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
