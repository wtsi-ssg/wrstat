package basedirs

import (
	"reflect"
	"strings"
	"testing"
)

func TestTSV(t *testing.T) {
	for n, test := range [...]struct {
		Input  string
		Output Config
		Error  string
	}{
		{
			Input: "some/path\t1\t2\nsome/other/path\t3\t4",
			Output: Config{
				{
					Prefix:  "some/path",
					Splits:  1,
					MinDirs: 2,
				},
				{
					Prefix:  "some/other/path",
					Splits:  3,
					MinDirs: 4,
				},
			},
		},
		{
			Input: "some/path\t12\nsome/other/path\t3\t4",
			Error: "bad TSV",
		},
	} {
		c, err := ParseConfig(strings.NewReader(test.Input))
		if err != nil {
			if errStr := err.Error(); errStr != test.Error {
				t.Errorf("test %d: expecting error %q, got %q", n+1, test.Error, errStr)
			}
		} else if test.Error != "" {
			t.Errorf("test %d: expecting error %q, got none", n+1, test.Error)
		} else if !reflect.DeepEqual(c, test.Output) {
			t.Errorf("test %d: expecting output %#v, got %#v", n+1, test.Output, c)
		}
	}
}
