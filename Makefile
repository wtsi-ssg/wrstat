PKG := github.com/wtsi-ssg/wrstat/v6
VERSION := $(shell git describe --tags --always --long --dirty)
TAG := $(shell git describe --abbrev=0 --tags)
LDFLAGS = -X ${PKG}/cmd.Version=${VERSION}
export GOPATH := $(shell go env GOPATH)
PATH := $(PATH):${GOPATH}/bin

default: install

# We require CGO_ENABLED=1 for getting group information to work properly; the
# pure go version doesn't work on all systems such as those using LDAP for
# groups
export CGO_ENABLED = 1

build:
	go build -tags netgo ${LDFLAGS}

buildsplit:
	go build -tags walk -ldflags "${LDFLAGS}" -o wrstat-split
	go build -tags walk,stat -ldflags "${LDFLAGS}" -o wrstat-split-walk
	go build -tags netgo,stat -ldflags "${LDFLAGS}" -o wrstat-split-stat

install:
	@rm -f ${GOPATH}/bin/wrstat
	@go install -tags netgo -ldflags "${LDFLAGS}"
	@echo installed to ${GOPATH}/bin/wrstat

test:
	@go test -tags netgo --count 1 ./...

race:
	go test -tags netgo -race --count 1 ./...

racesplit: export WRSTAT_TEST_SPLIT = 1
racesplit:
	go test -tags netgo -race --count 1 ./...

bench:
	go test -tags netgo --count 1 -run Bench -bench=. ./...

# curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.59.1
lint:
	@golangci-lint run --timeout 2m

clean:
	@rm -f ./wrstat
	@rm -f ./dist.zip

# go install github.com/goreleaser/goreleaser/v2@2.9.0
dist: export WRSTAT_LDFLAGS = $(LDFLAGS)
dist:
	goreleaser release --clean

.PHONY: test race bench lint build install clean dist
