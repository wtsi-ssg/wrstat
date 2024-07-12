package basedirs

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/wtsi-ssg/wrstat/v4/internal/split"
)

type ConfigAttrs struct {
	Prefix  []string
	Splits  uint64
	MinDirs uint64
}

type Config []ConfigAttrs

var (
	ErrBadTSV = errors.New("bad TSV")

	slashByte   = []byte{'/'}  //nolint:gochecknoglobals
	newLineByte = []byte{'\n'} //nolint:gochecknoglobals
	tabByte     = []byte{'\t'} //nolint:gochecknoglobals
)

const (
	numColumns = 3
	noMatch    = -1

	DefaultSplits  = 1
	defaultMinDirs = 2
)

func ParseConfig(r io.Reader) (Config, error) {
	b := bufio.NewReader(r)

	var ( //nolint:prealloc
		result Config
		end    bool
	)

	for !end {
		line, err := b.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			end = true
		} else if err != nil {
			return nil, err
		}

		line = bytes.TrimSuffix(line, newLineByte)

		conf, err := parseLine(line)
		if err != nil {
			return nil, err
		}

		result = append(result, conf)
	}

	return result, nil
}

func parseLine(line []byte) (conf ConfigAttrs, err error) {
	attr := bytes.Split(line, tabByte)
	if len(attr) != numColumns {
		return conf, ErrBadTSV
	}

	conf.Prefix = strings.Split(string(bytes.TrimPrefix(attr[0], slashByte)), "/")

	conf.Splits, err = strconv.ParseUint(string(attr[1]), 10, 0)
	if err != nil {
		return conf, err
	}

	conf.MinDirs, err = strconv.ParseUint(string(attr[2]), 10, 0)
	if err != nil {
		return conf, err
	}

	return conf, nil
}

func (c *Config) splitFn() split.SplitFn {
	return func(path string) int {
		return int(c.findBestMatch(path).Splits)
	}
}

func (c *Config) findBestMatch(path string) ConfigAttrs {
	maxScore := -1
	conf := ConfigAttrs{
		Splits:  DefaultSplits,
		MinDirs: defaultMinDirs,
	}

	pathParts := strings.Split(strings.TrimPrefix(path, "/"), "/")

	for _, p := range *c {
		if score := p.scoreMatch(pathParts); score > maxScore {
			maxScore = score
			conf = p
		}
	}

	return conf
}

func (p *ConfigAttrs) scoreMatch(pathParts []string) int {
	if len(pathParts) < len(p.Prefix) {
		return noMatch
	}

	var score int

	for i, prefixPart := range p.Prefix {
		pathPart := pathParts[i]

		if prefixPart == pathPart {
			score++
		} else if match, _ := filepath.Match(prefixPart, pathPart); !match { //nolint:errcheck
			return noMatch
		}

		score++
	}

	return score
}
