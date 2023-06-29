import {formatBytes, formatNumber} from './format';
import type {History} from './rpc';

const formatDate = (dStr: string | number) => {
	const d = new Date(dStr);

	return `${
		d.getFullYear()
	}-${
		(d.getMonth() + 1 + "").padStart(2, "0")
	}-${
		(d.getDate() + "").padStart(2, "0")
	}`;
}

export default ({history, width, height}: {history: History[], width: number, height: number}) => {
	let minDate = Infinity,
	maxDate = -Infinity,
	maxSize = 0,
	quotaPath = "M",
	sizePath = "M";

	if (history.length === 0) {
		return <></>
	}

	const quotaPoints: JSX.Element[] = [],
	sizePoints: JSX.Element[] = [];

	for (const h of history) {
		if (h.QuotaSize > maxSize) {
			maxSize = h.QuotaSize;
		}

		if (h.UsageSize > maxSize) {
			maxSize = h.UsageSize;
		}

		const d = new Date(h.Date).valueOf();

		if (d < minDate) {
			minDate = d;
		}

		if (d > maxDate) {
			maxDate = d;
		}
	}

	let first = true;

	minDate -= (maxDate - minDate) / 10;
	maxDate += (maxDate - minDate) / 10;

	const order = Math.pow(10, Math.max(Math.floor(Math.log10(maxSize)), 1));
	maxSize = order * Math.ceil(maxSize / order);

	const paddingXL = 60,
	paddingXR = 10,
	paddingYT = 10,
	paddingYB = 40,
	dateDiff = maxDate - minDate,
	xScale = (width - paddingXL - paddingXR) / dateDiff,
	yScale = (height - paddingYT - paddingYB) / maxSize,
	maxY = maxSize * yScale;

	for (const h of history) {
		const d = new Date(h.Date).valueOf(),
		x = paddingXL + (d - minDate) * xScale,
		quotaY = paddingYT + maxY - h.QuotaSize * yScale,
		sizeY = paddingYT + maxY - h.UsageSize * yScale;

		if (!first) {
			quotaPath += " L"
			sizePath += " L";
		}

		quotaPath += `${x},${quotaY}`;
		sizePath += `${x},${sizeY}`;

		quotaPoints.push(<use href="#point" fill="#00c9cf" x={x} y={quotaY}><title>{formatBytes(h.QuotaSize) + "\n" + formatNumber(h.QuotaSize) + " Bytes\n" + formatDate(h.Date)}</title></use>)
		sizePoints.push(<use href="#point" fill="#fb8c80" x={x} y={sizeY}><title>{formatBytes(h.UsageSize) + "\n" + formatNumber(h.UsageSize) + " Bytes\n" + formatDate(h.Date)}</title></use>)

		first = false;
	}

	return <svg xmlns="http://www.w3.org/2000/svg" width={width} height={height} viewBox={`0 0 ${width} ${height}`}>
		<defs>
			<path id="point" d="M0,5 L5,0 0,-5 -5,0 Z" />
		</defs>
		<rect x={paddingXL} y={paddingYT} width={width - paddingXL - paddingXR} height={height - paddingYT - paddingYB} fill="#ddd" stroke="#000" />
		{
			Array.from({length: 4}, (_, n) => <line x1={paddingXL} x2={width - paddingXR} y1={paddingYT + yScale * (n + 1) * maxSize / 5} y2={paddingYT + yScale * (n + 1) * maxSize / 5} stroke="#fff" />)
		}
		{
			Array.from({length: 6}, (_, n) => <text x={paddingXL - 3} y={paddingYT + yScale * n * maxSize / 5 + 5} fill="#000" text-anchor="end"><title>{formatNumber(maxSize * (5 - n) / 5) + " Bytes"}</title>{formatBytes(maxSize * (5 - n) / 5)}</text>)
		}
		{
			Array.from({length: 4}, (_, n) => <line x1={paddingXL + xScale * (n + 1) * dateDiff / 5} x2={paddingXL + xScale * (n + 1) * dateDiff / 5} y1={paddingYT} y2={height - paddingYB} stroke="#fff" />)
		}
		{
			Array.from({length: 6}, (_, n) => <text x={paddingXL + xScale * n * dateDiff / 5} y={height - paddingYB + 15} fill="#000" text-anchor={n === 0 ? "start" : n === 5 ? "end" : "middle"}>{formatDate(minDate + dateDiff * n / 5)}</text>)
		}
		<path d={quotaPath} stroke="#00c9cf" fill="none" />
		<path d={sizePath} stroke="#fb8c80" fill="none" />
		{quotaPoints}
		{sizePoints}
	</svg>
}