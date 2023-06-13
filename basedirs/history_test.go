/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
 *   Michael Woolnough <mw31@sanger.ac.uk>
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

package basedirs

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

const secondsInDay = time.Hour * 24

func TestHistory(t *testing.T) {
	now := time.Now()
	for _, test := range [...]struct {
		Name        string
		Histories   []History
		UntilSize   time.Time
		UntilInodes time.Time
	}{
		{
			Name:        "Zero history produces no dates",
			Histories:   nil,
			UntilSize:   time.Time{},
			UntilInodes: time.Time{},
		},
		{
			Name: "A Single item in History produces no dates",
			Histories: []History{
				{
					Date:        now,
					UsageSize:   2,
					QuotaSize:   10,
					UsageInodes: 1,
					QuotaInodes: 20,
				},
			},
			UntilSize:   time.Time{},
			UntilInodes: time.Time{},
		},
		{
			Name: "A Single item in History, with Quotas full produces now",
			Histories: []History{
				{
					Date:        now,
					UsageSize:   10,
					QuotaSize:   10,
					UsageInodes: 20,
					QuotaInodes: 20,
				},
			},
			UntilSize:   now,
			UntilInodes: now,
		},
		{
			Name: "A Single item in History, with Quotas over-full produces now",
			Histories: []History{
				{
					Date:        now,
					UsageSize:   20,
					QuotaSize:   10,
					UsageInodes: 30,
					QuotaInodes: 20,
				},
			},
			UntilSize:   now,
			UntilInodes: now,
		},
		{
			Name: "Two items in History produces useful predicted dates",
			Histories: []History{
				{
					Date:        now.Add(-24 * time.Hour),
					UsageSize:   5,
					QuotaSize:   100,
					UsageInodes: 0,
					QuotaInodes: 20,
				},
				{
					Date:        now,
					UsageSize:   20,
					QuotaSize:   100,
					UsageInodes: 10,
					QuotaInodes: 20,
				},
			},
			UntilSize:   now.Add(secondsInDay*5 + 8*time.Hour),
			UntilInodes: now.Add(secondsInDay * 1),
		},
		{
			Name: "Two items in history, with no change in size, and inodes at quota" +
				" produces no date for size and now for inodes",
			Histories: []History{
				{
					Date:        time.Now().Add(-25 * time.Hour),
					UsageSize:   5,
					QuotaSize:   100,
					UsageInodes: 0,
					QuotaInodes: 20,
				},
				{
					Date:        time.Now(),
					UsageSize:   5,
					QuotaSize:   100,
					UsageInodes: 20,
					QuotaInodes: 20,
				},
			},
			UntilSize:   time.Time{},
			UntilInodes: now,
		},
		{
			Name: "Two items in history, with a downward trend for size and inodes, produces no dates",
			Histories: []History{
				{
					Date:        time.Now().Add(-24 * time.Hour),
					UsageSize:   50,
					QuotaSize:   100,
					UsageInodes: 50,
					QuotaInodes: 20,
				},
				{
					Date:        time.Now(),
					UsageSize:   10,
					QuotaSize:   100,
					UsageInodes: 0,
					QuotaInodes: 20,
				},
			},
			UntilSize:   time.Time{},
			UntilInodes: time.Time{},
		},
		{
			Name: "Three items in history correctly uses the last and third from last items to predict dates.",
			Histories: []History{
				{
					Date:        time.Now().Add(-48 * time.Hour),
					UsageSize:   0,
					QuotaSize:   100,
					UsageInodes: 0,
					QuotaInodes: 20,
				},
				{
					Date:        time.Now().Add(-24 * time.Hour),
					UsageSize:   5,
					QuotaSize:   100,
					UsageInodes: 5,
					QuotaInodes: 20,
				},
				{
					Date:        time.Now(),
					UsageSize:   5,
					QuotaSize:   100,
					UsageInodes: 10,
					QuotaInodes: 20,
				},
			},
			UntilSize:   now.Add(secondsInDay * 38),
			UntilInodes: now.Add(secondsInDay * 2),
		},
		{
			Name: "Four items in history correctly uses the last and third from last items to predict dates.",
			Histories: []History{
				{
					Date:        time.Now().Add(-72 * time.Hour),
					UsageSize:   100,
					QuotaSize:   100,
					UsageInodes: 100,
					QuotaInodes: 20,
				},
				{
					Date:        time.Now().Add(-48 * time.Hour),
					UsageSize:   0,
					QuotaSize:   100,
					UsageInodes: 0,
					QuotaInodes: 20,
				},
				{
					Date:        time.Now().Add(-24 * time.Hour),
					UsageSize:   5,
					QuotaSize:   100,
					UsageInodes: 5,
					QuotaInodes: 20,
				},
				{
					Date:        time.Now(),
					UsageSize:   5,
					QuotaSize:   100,
					UsageInodes: 10,
					QuotaInodes: 20,
				},
			},
			UntilSize:   now.Add(secondsInDay * 38),
			UntilInodes: now.Add(secondsInDay * 2),
		},
	} {
		Convey(test.Name, t, func() {
			untilSize, untilInodes := DateQuotaFull(test.Histories)

			So(untilSize.Unix(), ShouldBeBetween, test.UntilSize.Unix()-2, test.UntilSize.Unix()+2)
			So(untilInodes.Unix(), ShouldBeBetween, test.UntilInodes.Unix()-2, test.UntilInodes.Unix()+2)
		})
	}
}
