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
	asDaysAgo = (date: string) => Math.max(0, Math.round((now - new Date(date).valueOf()) / msInDay)),
	asDaysAgoStr = (date: string) => formatNumber(asDaysAgo(date)),
	formatTimeAgo = (dStr: string | number) => {
		const secs = Math.round((Date.now() - new Date(dStr).valueOf()) / 1000);

		if (secs < 0.75 * minute) {
			return `${secs} ago`;
		} else if (secs < 1.5 * minute) {
			return "about a minute ago";
		} else if (secs < 0.75 * hour) {
			return `${Math.round(secs / minute)} minutes ago`;
		} else if (secs < 1.5 * hour) {
			return "about an hour ago";
		} else if (secs < day) {
			return `${Math.round(secs / hour)} hours ago`;
		} else if (secs < 1.75 * day) {
			return "about a day ago";
		} else if (secs < month) {
			return `${Math.round(secs / day)} days ago`;
		} else if (secs < 1.5 * month) {
			return "about a month ago";
		} else if (secs < year) {
			return `${Math.round(secs / month)} months ago`;
		} else if (secs < 1.5 * year) {
			return "about a year ago";
		} else {
			return `${Math.round(secs / year)} years ago`;
		}
	},
	formatDate = (dStr: string | number) => {
		const d = new Date(dStr);

		return `${d.getFullYear()
			}-${(d.getMonth() + 1 + "").padStart(2, "0")
			}-${(d.getDate() + "").padStart(2, "0")
			}`;
	};