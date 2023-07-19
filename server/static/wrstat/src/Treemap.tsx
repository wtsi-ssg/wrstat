import type { MouseEventHandler } from "react";

type TreeMapParams = {
	table: Table | null;
	width: number;
	height: number;
	noAuth?: boolean;
	onmouseout?: MouseEventHandler;
}

export type Entry = {
	key?: string;
	name: string;
	value: number;
	colour?: string;
	backgroundColour?: string;
	onclick?: MouseEventHandler<SVGRectElement>;
	onmouseover?: MouseEventHandler<SVGRectElement>;
	noauth: boolean;
}

export type Table = Entry[];

type Box = {
	left: number;
	top: number;
	right: number;
	bottom: number;
}

const phi = (1 + Math.sqrt(5)) / 2,
	buildTree = (table: Table, box: Box) => {
		let lastFontSize = Infinity,
			remainingTotal = 0,
			pos = 0;

		for (const e of table) {
			remainingTotal += e.value;
		}

		const toRet: JSX.Element[] = [];

		while (box.right - box.left >= 1 && box.bottom - box.top >= 1) {
			const isRow = (box.right - box.left) / (box.bottom - box.top) < phi || pos === 0,
				boxWidth = box.right - box.left,
				boxHeight = box.bottom - box.top;

			let total = table[pos].value,
				split = pos + 1,
				totalRatio = total / remainingTotal,
				lastDR = phi - boxWidth * (isRow ? 1 : totalRatio) / (boxHeight * (isRow ? totalRatio : 1)),
				d = isRow ? box.left : box.top;

			for (let i = split; i < table.length; i++) {
				const { value } = table[i],
					nextTotal = total + value,
					rowHeight = boxHeight * (isRow ? nextTotal / remainingTotal : value / nextTotal),
					colWidth = boxWidth * (isRow ? value / nextTotal : nextTotal / remainingTotal),
					dRatio = phi - colWidth / rowHeight;

				if ((isRow || lastDR < 0) && Math.abs(dRatio) > Math.abs(lastDR)) {
					break;
				}

				lastDR = dRatio;
				split++;
				total = nextTotal;
			}

			for (let i = pos; i < split; i++) {
				const entry = table[i],
					top = isRow ? box.top : d,
					left = isRow ? d : box.left,
					colWidth = isRow ? boxWidth * entry.value / total : boxWidth * total / remainingTotal,
					rowHeight = isRow ? boxHeight * total / remainingTotal :
						boxHeight * entry.value / total,
					bbox = getTextBB((entry.onclick ? "" : "W") + entry.name),
					xScale = colWidth / bbox.width,
					yScale = rowHeight / bbox.height,
					minScale = lastFontSize = Math.min(xScale, yScale, lastFontSize);

				d += isRow ? colWidth : rowHeight;

				toRet.push(
					<rect
						key={entry.key ? `rect_${entry.key}` : ""}
						x={left}
						y={top}
						width={colWidth}
						height={rowHeight}
						stroke="#000"
						fill={entry.backgroundColour ?? "#fff"}
						className={entry.onclick ? "hasClick box" : "box"}
						onClick={entry.onclick}
						onMouseOver={entry.onmouseover}
					><title>{entry.name}</title></rect>,
					entry.onclick ? <></> :
						entry.noauth ?
							<use
								key={entry.key ? `lock_${entry.key}` : ""}
								x={left + (colWidth - bbox.width * minScale * 0.9) / 2}
								y={top + (rowHeight - minScale * 0.40) / 2}
								href="#lock"
								width="0.5em"
								height="0.5em"
								style={{ fontSize: `${minScale * 0.9}px` }} /> :
							<use
								key={entry.key ? `empty_${entry.key}` : ""}
								x={left + (colWidth - bbox.width * minScale * 0.9) / 2}
								y={top + (rowHeight - minScale * 0.40) / 2}
								href="#emptyDirectory"
								width="0.5em"
								height="0.3846em"
								style={{ fontSize: `${minScale * 0.9}px` }} />,
					<text
						key={entry.key ? `text_${entry.key}` : ""}
						fontSize={minScale * 0.9}
						fontFamily={font}
						x={(entry.noauth ? minScale * 0.225 : 0) + left + colWidth / 2}
						y={top + rowHeight / 2 + 0.225 * minScale * bbox.height}
						textAnchor="middle"
						fill={entry.colour ?? "#000"}
					>{entry.name}</text>
				);
			}

			if (isRow) {
				box.top += boxHeight * total / remainingTotal;
			} else {
				box.left += boxWidth * total / remainingTotal;
			}

			pos = split;
			remainingTotal -= total;
		}

		return toRet;
	},
	font = "\"Helvetica Neue\", Helvetica, Arial, sans-serif",
	getTextBB = (() => {
		const ctx = document.createElement("canvas").getContext("2d");

		if (!ctx) {
			return () => ({ "width": 1, "height": 1 });
		}

		ctx.font = "1px " + font;
		ctx.textBaseline = "bottom";

		return (text: string) => {
			const { width = 1, actualBoundingBoxAscent: height = 1 } = ctx.measureText(text) ?? { "width": 1, "height": 1 };

			return { width, height };
		};
	})(),
	maxTableEntries = 1000,
	TreemapComponent = ({
		table,
		width,
		height,
		noAuth = false,
		onmouseout
	}: TreeMapParams) => {
		if (table === null) {
			return <></>
		}

		const filteredTable: Table = [];

		for (const entry of table) {
			if (entry.value > 0) {
				if (filteredTable.length === maxTableEntries) {
					break;
				}

				filteredTable.push(entry);
			}
		}

		if (filteredTable.length === 0) {
			return <svg className="treeMap" xmlns="http://www.w3.org/2000/svg" width={width} height={height} viewBox={`0 0 ${width} ${height}`}>
				<rect width="100%" height="100%" stroke="currentColor" style={{ fill: "var(--background)" }} />
				{
					noAuth ?
						<use href="#lock" height={150} transform={`translate(0 ${(height - 200) / 2})`} /> :
						<use href="#emptyDirectory" height={150} transform={`translate(0 ${(height - 200) / 2})`} />
				}
			</svg>;
		}

		const box: Box = {
			"left": 0,
			"top": 0,
			"right": width,
			"bottom": height
		};

		return <svg className="treeMap" xmlns="http://www.w3.org/2000/svg" width={width} height={height} viewBox={`0 0 ${width} ${height}`} onMouseOut={onmouseout}>
			{buildTree(table, box)}
		</svg>
	};

export default TreemapComponent;