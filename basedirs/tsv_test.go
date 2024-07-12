package basedirs

import (
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestTSV(t *testing.T) {
	Convey("", t, func() {
		for _, test := range [...]struct {
			Input  string
			Output Config
			Error  error
		}{
			{
				Input: "some/path\t1\t2\nsome/other/path\t3\t4",
				Output: Config{
					{
						Prefix:  []string{"some", "path"},
						Splits:  1,
						MinDirs: 2,
					},
					{
						Prefix:  []string{"some", "other", "path"},
						Splits:  3,
						MinDirs: 4,
					},
				},
			},
			{
				Input: "some/path\t12\nsome/other/path\t3\t4",
				Error: ErrBadTSV,
			},
		} {
			c, err := ParseConfig(strings.NewReader(test.Input))
			So(err, ShouldEqual, test.Error)
			So(c, ShouldResemble, test.Output)
		}
	})
}

func TestSplitFn(t *testing.T) {
	c := Config{
		{
			Prefix: []string{"ab", "cd"},
			Splits: 3,
		},
		{
			Prefix: []string{"ab", "ef"},
			Splits: 2,
		},
		{
			Prefix: []string{"some", "*", "other", "path"},
			Splits: 4,
		},
		{
			Prefix: []string{"some", "*", "other", "*", "longerpath"},
			Splits: 5,
		},
		{
			Prefix: []string{"some", "partial*", "thing"},
			Splits: 6,
		},
	}

	fn := c.splitFn()

	Convey("", t, func() {
		for _, test := range [...]struct {
			Input  string
			Output int
		}{
			{
				"/ab/cd/ef",
				3,
			},
			{
				"/ab/cd/ef/gh",
				3,
			},
			{
				"/some/thins/other/path/p",
				4,
			},
			{
				"/some/thins/other/wombat/longerpath",
				5,
			},
			{
				"/some/partial/thing",
				6,
			},
			{
				"/some/partialCat/thing",
				6,
			},
		} {
			out := fn(test.Input)
			So(test.Output, ShouldEqual, out)
		}
	})
}
