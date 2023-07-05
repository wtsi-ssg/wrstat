import {asDaysAgo, formatBytes} from "./format";

type Data = {
	UsageSize: number;
	Mtime: string;
}

const minDaysAgo = (date: string) => {
	const daysAgo = asDaysAgo(date);

	if (daysAgo < 0) {
		return 0;
	}

	return daysAgo;
}

export default ({data, width, height, logX = false, logY = false}: {data: Data[], width: number, height: number, logX?: boolean, logY?: boolean}) => {
	const paddingXL = 80,
	paddingXR = 10,
	paddingYT = 10,
	paddingYB = 65,
	innerPadding = 10,
	graphWidth = width - paddingXL - paddingXR - 2 * innerPadding,
	graphHeight = height - paddingYT - paddingYB - 2 * innerPadding,
	sizeToY = (size: number, log = logY) => paddingYT + innerPadding + (log ? graphHeight - graphHeight * (Math.log(1 + size) / Math.log(1 + maxSize)) : yScale * (maxSize - size)),
	dateToX = (days: number, log = logX) => paddingXL + innerPadding + (log ? graphWidth * (Math.log(1 + days)  / Math.log(1 + maxDate)) : xScale * days);

	let maxSize = -Infinity,
	maxDate = -Infinity;

	for (const d of data) {
		if (d.UsageSize > maxSize) {
			maxSize = d.UsageSize;
		}

		const daysAgo = minDaysAgo(d.Mtime);

		if (daysAgo > maxDate) {
			maxDate = daysAgo;
		}
	}

	maxDate += maxDate / 20;
	maxSize += maxSize / 20;

	const xScale = graphWidth / maxDate,
	yScale = graphHeight / maxSize;

	return <svg id="scatter" xmlns="http://www.w3.org/2000/svg" width={width} height={height} viewBox={`0 0 ${width} ${height}`}>
		<defs>
			<circle id="marker" r="2" fill="currentColor" fillOpacity="0.25" />
		</defs>
		<rect x={paddingXL} y={paddingYT} width={graphWidth + 2 * innerPadding} height={graphHeight + 2 * innerPadding} style={{"fill": "var(--historyBack, #ddd)"}} stroke="currentColor" />
		{
			Array.from({length: 6}, (_, n) => <line x1={dateToX(0, false) - innerPadding} x2={dateToX(maxDate, false) + innerPadding} y1={sizeToY(maxSize * n / 5, false)} y2={sizeToY(maxSize * n / 5, false)} stroke="#fff" />)
		}
		{
			Array.from({length: 6}, (_, n) => <text x={dateToX(0, false) - innerPadding - 5} y={Math.max(sizeToY(maxSize * n / 5, false), paddingYT) + 5} fill="currentColor" textAnchor="end">{formatBytes(Math.round(logY ? Math.pow(Math.E, Math.log(1 + maxSize) * n / 5) - 1 : maxSize * n / 5))}</text>)
		}
		{
			Array.from({length: 6}, (_, n) => <line x1={dateToX(maxDate * n / 5, false)} x2={dateToX(maxDate * n / 5, false)} y1={sizeToY(maxSize, false) - innerPadding} y2={sizeToY(0, false) + innerPadding} stroke="#fff" />)
		}
		{
			Array.from({length: 6}, (_, n) => <text x={-10} y={20} transform={`translate(${dateToX(maxDate * n / 5, false)} ${sizeToY(0, false)}) rotate(-45)`} fill="currentColor" textAnchor="end">{Math.round(logX ? Math.pow(Math.E, Math.log(1 + maxDate) * n / 5) - 1 : maxDate * n / 5)}</text>)
		}
		{
			data.map(d => <use href="#marker" x={dateToX(minDaysAgo(d.Mtime))} y={sizeToY(d.UsageSize)} />)
		}
		<text x={paddingXL + (width - paddingXL - paddingXR) / 2} y={height - 5} textAnchor="middle">Last Modified (Days)</text>
	</svg>
}