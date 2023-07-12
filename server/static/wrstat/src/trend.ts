import type { History } from './rpc';

const calculateTrend = (max: number, latestTime: number, oldestTime: number, latestValue: number, oldestValue: number) => {
	if (latestValue >= max) {
		return 0;
	}

	if (latestTime === oldestTime || latestValue <= oldestValue) {
		return Infinity;
	}

	const dt = latestTime - oldestTime,
		dy = latestValue - oldestValue,
		c = latestValue - latestTime * dy / dt,
		secs = (max - c) * dt / dy;

	return secs;
},
	day = 86_400,
	limit = 3 * day;

export const exceedDates = (history: History[]): [number, number] => {
	if (history.length === 0) {
		return [0, 0];
	}

	const latest = history[history.length - 1],
		oldest = history[Math.max(history.length - 3, 0)],
		latestDate = new Date(latest.Date).valueOf() / 1000,
		oldestDate = new Date(oldest.Date).valueOf() / 1000,
		untilSize = calculateTrend(latest.QuotaSize, latestDate, oldestDate, latest.UsageSize, oldest.UsageSize),
		untilInodes = calculateTrend(latest.QuotaInodes, latestDate, oldestDate, latest.UsageInodes, oldest.UsageInodes);

	return [Math.round(untilSize / day), Math.round(untilInodes / day)];
}

const TrendComponent = (history: History[]) => {
	if (history.length < 1) {
		return null;
	}

	const [untilSize, untilInodes] = exceedDates(history);

	return untilInodes < limit || untilSize < limit;
};

export default TrendComponent;