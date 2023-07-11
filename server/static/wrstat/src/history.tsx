import {useEffect, useState} from 'react';
import {formatDate} from './format';

type UsageHistory = {
	Quota: number;
	Usage: number;
	Date: string;
}

export default ({history, width, height, yFormatter, secondaryFormatter, yRounder}: {history: UsageHistory[], width: number, height: number, yFormatter: (num: number) => string, secondaryFormatter: (num: number) => string, yRounder: (num: number) => number}) => {
	const [infoBox, setInfoBox] = useState(-1),
	amountToY = (amount: number) => paddingYT + maxY - amount * yScale,
	dateToX = (date: number) => paddingXL + (date - minDate) * xScale;

	let minDate = Infinity,
	maxDate = -Infinity,
	maxAmount = 0,
	quotaPath = "M",
	usagePath = "M";

	useEffect(() => setInfoBox(-1), [history, width, height]);

	if (history.length === 0) {
		return <></>
	}

	const quotaPoints: JSX.Element[] = [],
	usagePoints: JSX.Element[] = [],
	infoBoxes: JSX.Element[] = [];

	for (const h of history) {
		if (h.Quota > maxAmount) {
			maxAmount = h.Quota;
		}

		if (h.Usage > maxAmount) {
			maxAmount = h.Usage;
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

	maxAmount = Math.max(yRounder(maxAmount), 5);

	const paddingXL = 80,
	paddingXR = 80,
	paddingYT = 10,
	paddingYB = 70,
	dateDiff = maxDate - minDate,
	xScale = (width - paddingXL - paddingXR) / dateDiff,
	yScale = (height - paddingYT - paddingYB) / maxAmount,
	maxY = maxAmount * yScale;

	let first = true;

	for (const h of history) {
		const d = new Date(h.Date).valueOf(),
		x = dateToX(d),
		quotaY = amountToY(h.Quota),
		sizeY = amountToY(h.Usage),
		quotaBox = infoBoxes.push(<div style={{left: x + "px", top: quotaY + "px", display: infoBoxes.length === infoBox ? "inline-block" : ""}}>
			{yFormatter(h.Quota)}
			<br />
			{secondaryFormatter(h.Quota)}
			<br />
			{formatDate(h.Date)}
		</div>) - 1,
		sizeBox = infoBoxes.push(<div style={{left: x + "px", top: sizeY + "px", display: infoBoxes.length === infoBox ? "inline-block" : ""}}>
			{yFormatter(h.Usage)}
			<br />
			{secondaryFormatter(h.Usage)}
			<br />
			{formatDate(h.Date)}
		</div>) - 1;

		if (!first) {
			quotaPath += " L"
			usagePath += " L";
		}

		quotaPath += `${x},${quotaY}`;
		usagePath += `${x},${sizeY}`;

		quotaPoints.push(<use href="#point" fill="#00c9cf" x={x} y={quotaY} onMouseOver={() => setInfoBox(quotaBox)} onMouseOut={() => setInfoBox(-1)} />)
		usagePoints.push(<use href="#point" fill="#fb8c80" x={x} y={sizeY} onMouseOver={() => setInfoBox(sizeBox)} onMouseOut={() => setInfoBox(-1)} />)

		first = false;
	}

	const previousHistory = history.at(-3) ?? history.at(0)!,
	latestHistory = history.at(-1)!,
	previousDate = new Date(previousHistory.Date).valueOf(),
	latestDate = new Date(latestHistory.Date).valueOf(),
	dt = latestDate - previousDate,
	dy = latestHistory.Usage - previousHistory.Usage,
	m = dy / (dt || 1),
	c = latestHistory.Usage - latestDate * m;

	let x = projectDate,
	y = m * projectDate + c;

	if (latestHistory.Usage > latestHistory.Quota) {
		y = -Infinity;
	} else if (y < 0) {
		x = -c / m;
		y = 0;
	} else if (y > latestHistory.Quota) {
		x = (latestHistory.Quota - c) / m;
		y = latestHistory.Quota;

		infoBoxes.push(<div style={{left: dateToX(x) + "px", top: amountToY(maxY) + "px", display: infoBoxes.length === infoBox ? "inline-block" : ""}}>
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
			<rect x={paddingXL} y={paddingYT} width={width - paddingXL - paddingXR} height={height - paddingYT - paddingYB} style={{"fill": "var(--graphBack, #ddd)"}} stroke="currentColor" />
			{
				Array.from({length: 4}, (_, n) => <line x1={paddingXL} x2={width - paddingXR} y1={amountToY((n + 1) * maxAmount / 5)} y2={amountToY((n + 1) * maxAmount / 5)} stroke="#fff" />)
			}
			{
				Array.from({length: 6}, (_, n) => <text x={paddingXL - 3} y={amountToY(maxAmount - (n * maxAmount) / 5) + 5} fill="currentColor" text-anchor="end"><title>{secondaryFormatter(maxAmount * (5 - n) / 5)}</title>{yFormatter(maxAmount * (5 - n) / 5)}</text>)
			}
			{
				Array.from({length: 4}, (_, n) => <line x1={dateToX(minDate + (n + 1) * dateDiff / 5)} x2={dateToX(minDate + (n + 1) * dateDiff / 5)} y1={paddingYT} y2={height - paddingYB} stroke="#fff" />)
			}
			{
				Array.from({length: 6}, (_, n) => <text transform={`translate(${dateToX(minDate + n * dateDiff / 5)} ${height - paddingYB + 15}) rotate(-45)`} fill="currentColor" text-anchor="end">{formatDate(minDate + dateDiff * n / 5)}</text>)
			}
			<path d={quotaPath} stroke="#00c9cf" fill="none" />
			<path d={`M${dateToX(latestDate)},${amountToY(latestHistory.Quota)} L${dateToX(projectDate)},${amountToY(latestHistory.Quota)}`} stroke="#00c9cf" fill="none" stroke-width="3" stroke-dasharray="3" />
			<path d={usagePath} stroke="#fb8c80" fill="none" />
			<path d={`M${dateToX(latestDate)},${amountToY(latestHistory.Usage)} L${dateToX(x)},${amountToY(y)}`} stroke="#fb8c80" fill="none" stroke-width="3" stroke-dasharray="3" />
			{quotaPoints}
			{usagePoints}
			{
				y === latestHistory.Quota ? <path d="M5,5 L-5,-5 M-5,5 L5,-5" stroke="#f00" stroke-width={2} transform={`translate(${paddingXL + (x - minDate) * xScale} ${amountToY(y)})`} onMouseOver={() => setInfoBox(infoBoxes.length -1)} onMouseOut={() => setInfoBox(-1)} /> : []
			}
			<rect x={width - 70} y={paddingYT + 10} width={65} height={43} stroke="currentColor" fill="none" />
			<rect x={width - 65} y={paddingYT + 15} width={10} height={10} stroke="currentColor" fill="#00c9cf" />
			<text x={width - 47} y={paddingYT + 25} fill="currentColor">Quota</text>
			<rect x={width - 65} y={paddingYT + 35} width={10} height={10} stroke="currentColor" fill="#fb8c80" />
			<text x={width - 47} y={paddingYT + 45} fill="currentColor">Usage</text>
		</svg>
	</>
}