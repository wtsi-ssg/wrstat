package encode

import "testing"

func TestEncodeDecode(t *testing.T) {
	for n, test := range [...]string{
		"",
		"abc",
		"/some/path/",
	} {
		if output, err := Base64Decode(Base64Encode(test)); err != nil {
			t.Errorf("test %d: unexpected error: %s", n+1, err)
		} else if output != test {
			t.Errorf("test %d: expected output %q, got %q", n+1, test, output)
		}
	}
}
