const numberFormatter = new Intl.NumberFormat("en-GB", {"style": "decimal"}),
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
] as const;

export const formatNumber = (n: number) => numberFormatter.format(n),
formatBytes = (n: number) => {
	let unit = 0;
	
	while (n >= 1024) {
		unit++;
		n = Math.round(n / 1024);
	}

	return formatNumber(n) + " " + byteUnits[unit];
},
asGB = (num: number) => formatNumber(Math.round(num / (1024 * 1024 * 10.24)) / 100),
asDaysAgo = (date: string) => formatNumber(Math.max(0, Math.round((now - new Date(date).valueOf()) / msInDay)));