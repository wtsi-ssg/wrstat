/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
 *
 * Authors:
 *   Michael Woolnough <mw31@sanger.ac.uk>
 *   Sendu Bala <sb10@sanger.ac.uk>
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

import * as dateFns from "date-fns";

const numberFormatter = new Intl.NumberFormat("en-GB", { "style": "decimal" }),
	largeNumberFormatter = new Intl.NumberFormat("en-GB", {
		"style": "decimal",
		"notation": "compact",
	}),
	now = Date.now(),
	msInDay = 86400000,
	byteUnits = [
		"B",
		"KiB",
		"MiB",
		"GiB",
		"TiB",
		"PiB",
		"EiB",
		"ZiB",
		"YiB",
		"RiB",
		"QiB",
	] as const,
	minute = 60,
	hour = minute * 60,
	day = hour * 24,
	month = 30 * day,
	year = day * 365;

export const formatNumber = (n: number) => numberFormatter.format(n),
	formatLargeNumber = (n: number) => largeNumberFormatter.format(n),
	formatBytes = (n: number) => {
		let unit = 0;

		while (n >= 1024) {
			unit++;
			n = Math.round(n / 1024);
		}

		return formatNumber(n) + " " + byteUnits[unit];
	},
	asDaysAgo = (date: string) => Math.max(0, Math.floor((now - new Date(date).valueOf()) / msInDay)),
	asTimeAgo = (dateStr: string) => {
		const duration = dateFns.intervalToDuration({
			start: new Date(dateStr),
			end: new Date(),
		});

		let response = "";

		if (duration.years) {
			response += `${duration.years}y`;
		}
		if (duration.months) {
			response += `${duration.months}m`;
		}
		if (duration.days) {
			response += `${duration.days}d`;
		}

		return response || "<1d";
	},
	approxTimeAgo = (dStr: string | number) => {
		const secs = Math.round((Date.now() - new Date(dStr).valueOf()) / 1000);

		if (secs < 0.75 * minute) {
			return `${secs} ago`;
		} else if (secs < 1.5 * minute) {
			return "~ a minute ago";
		} else if (secs < 0.75 * hour) {
			return `~ ${Math.round(secs / minute)} minutes ago`;
		} else if (secs < 1.5 * hour) {
			return "~ an hour ago";
		} else if (secs < day) {
			return `~ ${Math.round(secs / hour)} hours ago`;
		} else if (secs < 1.75 * day) {
			return "~ a day ago";
		} else if (secs < month) {
			return `~ ${Math.round(secs / day)} days ago`;
		} else if (secs < 1.5 * month) {
			return "~ a month ago";
		} else if (secs < year) {
			return `~ ${Math.round(secs / month)} months ago`;
		} else if (secs < 1.5 * year) {
			return "~ a year ago";
		} else {
			return `~ ${Math.round(secs / year)} years ago`;
		}
	},
	formatDate = (dStr: string | number) => {
		const d = new Date(dStr);

		return `${d.getFullYear()
			}-${(d.getMonth() + 1 + "").padStart(2, "0")
			}-${(d.getDate() + "").padStart(2, "0")
			}`;
	},
	asBasename = (absPath: string) => {
		const basename = absPath.split('/').pop();

		return basename || "-";
	};