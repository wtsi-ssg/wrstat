package fixtimes

import (
	"time"
)

func FixTime(t time.Time) time.Time {
	_, offset := time.Now().Zone()
	l := time.FixedZone("", offset)

	return t.In(l)
}
