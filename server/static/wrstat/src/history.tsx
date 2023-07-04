import {useEffect, useState} from 'react';
import {formatBytes, formatDate, formatNumber} from './format';
import type {History} from './rpc';

export default ({history, width, height}: {history: History[], width: number, height: number}) => {
	const [infoBox, setInfoBox] = useState(-1);

	let minDate = Infinity,
	maxDate = -Infinity,
	maxSize = 0,
	quotaPath = "M",
	sizePath = "M";

	useEffect(() => setInfoBox(-1), [history, width, height]);

	if (history.length === 0) {
		return <></>
	}

	const quotaPoints: JSX.Element[] = [],
	sizePoints: JSX.Element[] = [],
	infoBoxes: JSX.Element[] = [];

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

	const projectDate = maxDate = maxDate += (maxDate - minDate) / 5;

	minDate -= (maxDate - minDate) / 10;
	maxDate += (maxDate - minDate) / 10;

	maxSize = 100 * Math.pow(2, Math.ceil(Math.log2(maxSize / 100)));

	const paddingXL = 80,
	paddingXR = 80,
	paddingYT = 10,
	paddingYB = 40,
	dateDiff = maxDate - minDate,
	xScale = (width - paddingXL - paddingXR) / dateDiff,
	yScale = (height - paddingYT - paddingYB) / maxSize,
	maxY = maxSize * yScale;

	let first = true;

	for (const h of history) {
		const d = new Date(h.Date).valueOf(),
		x = paddingXL + (d - minDate) * xScale,
		quotaY = paddingYT + maxY - h.QuotaSize * yScale,
		sizeY = paddingYT + maxY - h.UsageSize * yScale,
		quotaBox = infoBoxes.push(<div style={{left: x + "px", top: quotaY + "px", display: infoBoxes.length === infoBox ? "inline-block" : ""}}>
			{formatBytes(h.QuotaSize)}
			<br />
			{formatNumber(h.QuotaSize)} Bytes
			<br />
			{formatDate(h.Date)}
		</div>) - 1,
		sizeBox = infoBoxes.push(<div style={{left: x + "px", top: sizeY + "px", display: infoBoxes.length === infoBox ? "inline-block" : ""}}>
			{formatBytes(h.UsageSize)}
			<br />
			{formatNumber(h.UsageSize)} Bytes
			<br />
			{formatDate(h.Date)}
		</div>) - 1;

		if (!first) {
			quotaPath += " L"
			sizePath += " L";
		}

		quotaPath += `${x},${quotaY}`;
		sizePath += `${x},${sizeY}`;

		quotaPoints.push(<use href="#point" fill="#00c9cf" x={x} y={quotaY} onMouseOver={() => setInfoBox(quotaBox)} onMouseOut={() => setInfoBox(-1)} />)
		sizePoints.push(<use href="#point" fill="#fb8c80" x={x} y={sizeY} onMouseOver={() => setInfoBox(sizeBox)} onMouseOut={() => setInfoBox(-1)} />)

		first = false;
	}

	const previousHistory = history.at(-3) ?? history.at(0)!,
	latestHistory = history.at(-1)!,
	previousDate = new Date(previousHistory.Date).valueOf(),
	latestDate = new Date(latestHistory.Date).valueOf(),
	dt = latestDate - previousDate,
	dy = latestHistory.UsageSize - previousHistory.UsageSize,
	m = dy / (dt || 1),
	c = latestHistory.UsageSize - latestDate * m;

	let x = projectDate,
	y = m * projectDate + c;

	if (latestHistory.UsageSize > latestHistory.QuotaSize) {
		y = -Infinity;
	} else if (y < 0) {
		x = -c / m;
		y = 0;
	} else if (y > latestHistory.QuotaSize) {
		x = (latestHistory.QuotaSize - c) / m;
		y = latestHistory.QuotaSize;

		infoBoxes.push(<div style={{left: paddingXL + (x - minDate) * xScale + "px", top: paddingYT + maxY - y * yScale + "px", display: infoBoxes.length === infoBox ? "inline-block" : ""}}>
			Fill Quota Data: {formatDate(x)}
		</div>)
	}

	return <>
		<div id="historyInfo">
			{infoBoxes}
		</div>
		<svg id="history" xmlns="http://www.w3.org/2000/svg" width={width} height={height} viewBox={`0 0 ${width} ${height}`}>
			<defs>
				<path id="point" d="M0,5 L5,0 0,-5 -5,0 Z" />
			</defs>
			<rect x={paddingXL} y={paddingYT} width={width - paddingXL - paddingXR} height={height - paddingYT - paddingYB} style={{"fill": "var(--historyBack, #ddd)"}} stroke="currentColor" />
			{
				Array.from({length: 4}, (_, n) => <line x1={paddingXL} x2={width - paddingXR} y1={paddingYT + yScale * (n + 1) * maxSize / 5} y2={paddingYT + yScale * (n + 1) * maxSize / 5} stroke="#fff" />)
			}
			{
				Array.from({length: 6}, (_, n) => <text x={paddingXL - 3} y={paddingYT + yScale * n * maxSize / 5 + 5} fill="currentColor" text-anchor="end"><title>{formatNumber(maxSize * (5 - n) / 5) + " Bytes"}</title>{formatBytes(maxSize * (5 - n) / 5)}</text>)
			}
			{
				Array.from({length: 4}, (_, n) => <line x1={paddingXL + xScale * (n + 1) * dateDiff / 5} x2={paddingXL + xScale * (n + 1) * dateDiff / 5} y1={paddingYT} y2={height - paddingYB} stroke="#fff" />)
			}
			{
				Array.from({length: 6}, (_, n) => <text x={paddingXL + xScale * n * dateDiff / 5} y={height - paddingYB + 15} fill="currentColor" text-anchor={n === 0 ? "start" : n === 5 ? "end" : "middle"}>{formatDate(minDate + dateDiff * n / 5)}</text>)
			}
			<path d={quotaPath} stroke="#00c9cf" fill="none" />
			<path d={`M${paddingXL + (latestDate - minDate) * xScale},${paddingYT + maxY - latestHistory.QuotaSize * yScale} L${paddingXL + (projectDate - minDate) * xScale},${paddingYT + maxY - latestHistory.QuotaSize * yScale}`} stroke="#00c9cf" fill="none" stroke-width="3" stroke-dasharray="3" />
			<path d={sizePath} stroke="#fb8c80" fill="none" />
			<path d={`M${paddingXL + (latestDate - minDate) * xScale},${paddingYT + maxY - latestHistory.UsageSize * yScale} L${paddingXL + (x - minDate) * xScale},${paddingYT + maxY - y * yScale}`} stroke="#fb8c80" fill="none" stroke-width="3" stroke-dasharray="3" />
			{quotaPoints}
			{sizePoints}
			{
				y === latestHistory.QuotaSize ? <path d="M5,5 L-5,-5 M-5,5 L5,-5" stroke="#f00" stroke-width={2} transform={`translate(${paddingXL + (x - minDate) * xScale} ${paddingYT + maxY - y * yScale})`} onMouseOver={() => setInfoBox(infoBoxes.length -1)} onMouseOut={() => setInfoBox(-1)} /> : []
			}
			<rect x={width - 70} y={paddingYT + 10} width={65} height={43} stroke="currentColor" fill="none" />
			<rect x={width - 65} y={paddingYT + 15} width={10} height={10} stroke="currentColor" fill="#00c9cf" />
			<text x={width - 47} y={paddingYT + 25} fill="currentColor">Quota</text>
			<rect x={width - 65} y={paddingYT + 35} width={10} height={10} stroke="currentColor" fill="#fb8c80" />
			<text x={width - 47} y={paddingYT + 45} fill="currentColor">Usage</text>
		</svg>
	</>
}