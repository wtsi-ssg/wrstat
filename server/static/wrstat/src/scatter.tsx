import {useEffect, useState} from "react";
import {asDaysAgo, formatBytes, formatNumber} from "./format";

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
};

export default ({data, width, height, logX = false, logY = false, setLimits}: {data: Data[], width: number, height: number, logX?: boolean, logY?: boolean, setLimits: (minSize: number, maxSize: number, minDate: number, maxDate: number) => void}) => {
	const paddingXL = 80,
	paddingXR = 10,
	paddingYT = 10,
	paddingYB = 65,
	innerPadding = 10,
	graphWidth = width - paddingXL - paddingXR - 2 * innerPadding,
	graphHeight = height - paddingYT - paddingYB - 2 * innerPadding,
	sizeToY = (size: number, log = logY) => paddingYT + innerPadding + (log ? graphHeight - graphHeight * (Math.log(1 + size) / Math.log(1 + maxSize)) : yScale * (maxSize - size)),
	dateToX = (days: number, log = logX) => paddingXL + innerPadding + (log ? graphWidth * (Math.log(1 + days)  / Math.log(1 + maxDate)) : xScale * days),
	[highlightCoords, setHighlightCoords] = useState<null | [number, number, number, number]>(null),
	onDrag = (e: React.MouseEvent<SVGSVGElement, MouseEvent>) => {
		const coords = e.currentTarget.getBoundingClientRect(),
		graphLeft = coords.left + paddingXL,
		graphTop = coords.top + paddingYT,
		startX = e.clientX - graphLeft,
		startY = e.clientY - graphTop;

		if (startX < 0 || startX > graphWidth + 2 * innerPadding || startY < 0 || startY > graphHeight + 2 * innerPadding) {
			return;
		}

		const mousemove = (e: MouseEvent) => {
			const x = e.clientX - graphLeft,
			y = e.clientY - graphTop,
			minX = Math.max(Math.min(x, startX), 0),
			maxX = Math.min(Math.max(x, startX), graphWidth + 2 * innerPadding),
			minY = Math.max(Math.min(y, startY), 0),
			maxY = Math.min(Math.max(y, startY), graphHeight + 2 * innerPadding);

			if (minX === maxX || minY === maxY) {
				setHighlightCoords(null);
				setLimits(-Infinity, Infinity, -Infinity, Infinity);

				return;
			}

			setHighlightCoords([minX, maxX - minX, minY, maxY - minY]);

			const fMinX = Math.max(0, minX - innerPadding) / graphWidth,
			fMaxX = Math.min(graphWidth, maxX - innerPadding) / graphWidth,
			fMinY = Math.max(0, graphHeight - maxY + innerPadding) / graphHeight,
			fMaxY = Math.min(graphHeight, graphHeight - minY + innerPadding) / graphHeight,
			minDaysAgo = Math.round(logX ? Math.pow(Math.E, fMinX * Math.log(1 + maxDate)) - 1 : fMinX * maxDate),
			maxDaysAgo = Math.round(logX ? Math.pow(Math.E, fMaxX * Math.log(1 + maxDate)) - 1 : fMaxX * maxDate),
			minFileSize = Math.round(logY ? Math.pow(Math.E, fMinY * Math.log(1 + maxSize)) - 1 : fMinY * maxSize),
			maxFileSize = Math.round(logY ? Math.pow(Math.E, fMaxY * Math.log(1 + maxSize)) - 1 : fMaxY * maxSize);

			setLimits(minFileSize, maxFileSize, minDaysAgo, maxDaysAgo);
		},
		mouseup = (e: MouseEvent) => {
			mousemove(e);
			window.removeEventListener("mousemove", mousemove);
		};

		window.addEventListener("mousemove", mousemove);
		window.addEventListener("mouseup", mouseup, {"once": true});
	};

	useEffect(() => {
		setHighlightCoords(null);
		setLimits(-Infinity, Infinity, -Infinity, Infinity);
	}, [JSON.stringify(data), logX, logY]);

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

	return <svg id="scatter" xmlns="http://www.w3.org/2000/svg" width={width} height={height} viewBox={`0 0 ${width} ${height}`} onMouseDown={onDrag}>
		<defs>
			<circle id="marker" r="2.5" fill="currentColor" fillOpacity="0.4" />
		</defs>
		<rect className="back" x={paddingXL} y={paddingYT} width={graphWidth + 2 * innerPadding} height={graphHeight + 2 * innerPadding} style={{"fill": "var(--graphBack, #ddd)"}} stroke="currentColor" />
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
			Array.from({length: 6}, (_, n) => <text x={-10} y={20} transform={`translate(${dateToX(maxDate * n / 5, false)} ${sizeToY(0, false)}) rotate(-45)`} fill="currentColor" textAnchor="end">{formatNumber(Math.round(logX ? Math.pow(Math.E, Math.log(1 + maxDate) * n / 5) - 1 : maxDate * n / 5))}</text>)
		}
		{
			highlightCoords ? <rect className="back" x={highlightCoords[0] + paddingXL} width={highlightCoords[1]} y={highlightCoords[2] + paddingYT} height={highlightCoords[3]} fill="#9cf" fillOpacity={0.25} stroke="#036" strokeOpacity={0.25} /> : []
		}
		{
			data.map(d => <use href="#marker" x={dateToX(minDaysAgo(d.Mtime))} y={sizeToY(d.UsageSize)} onClick={() => {
				setLimits(d.UsageSize, d.UsageSize, asDaysAgo(d.Mtime), asDaysAgo(d.Mtime));
				setHighlightCoords(null);
			}} />)
		}
		<text x={paddingXL + (width - paddingXL - paddingXR) / 2} y={height - 5} textAnchor="middle" fill="currentColor">Last Modified (Days)</text>
	</svg>
}