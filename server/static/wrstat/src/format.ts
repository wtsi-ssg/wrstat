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
	] as const;

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
	formatDateTime = (dStr: string | number) => {
		const d = new Date(dStr);

		return `${d.getFullYear()
			}-${(d.getMonth() + 1 + "").padStart(2, "0")
			}-${(d.getDate() + "").padStart(2, "0")
			} ${(d.getHours() + "").padStart(2, "0")
			}:${(d.getMinutes() + "").padStart(2, "")
			}:${(d.getSeconds() + "").padStart(2, "")
			}`;
	},
	formatDate = (dStr: string | number) => {
		const d = new Date(dStr);

		return `${d.getFullYear()
			}-${(d.getMonth() + 1 + "").padStart(2, "0")
			}-${(d.getDate() + "").padStart(2, "0")
			}`;
	};