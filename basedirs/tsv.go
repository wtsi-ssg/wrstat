package basedirs

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"
)

type ConfigAttrs struct {
	Prefix  string
	Splits  uint64
	MinDirs uint64
}

type Config []ConfigAttrs

var ErrBadTSV = errors.New("bad TSV")

const numColumns = 3

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

		line = bytes.TrimSuffix(line, []byte{'\n'})

		conf, err := parseLine(line)
		if err != nil {
			return nil, err
		}

		result = append(result, conf)
	}

	return result, nil
}

func parseLine(line []byte) (conf ConfigAttrs, err error) {
	attr := bytes.Split(line, []byte{'\t'})
	if len(attr) != numColumns {
		return conf, ErrBadTSV
	}

	conf.Prefix = string(attr[0])

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