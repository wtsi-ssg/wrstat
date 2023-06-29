import 'jest-canvas-mock';
import {render} from '@testing-library/react';
import TreeMap, {type Table} from './treemap';

const testData: Table = [
	{
		name: "A",
		value: 60
	},
	{
		name: "B",
		value: 15
	},
	{
		name: "C",
		value: 15
	},
	{
		name: "D",
		value: 10
	},
	{
		name: "E",
		value: 0
	},
];

type Box = {
	top: number;
	left: number;
	bottom: number;
	right: number;
}

it("TreeMap creates boxes with the correct area", () => {
	const {asFragment} = render(<TreeMap width={10} height={10} table={testData} />),
	expectedAreas = new Map<string, number>(testData.map(({name, value}) => [name, value])),
	boxesGot: Box[] = [],
	overlapsExisting = (box: Box) => {
		let overlap = false;

		for (const existing of boxesGot) {
			if (box.left < existing.right && box.right > existing.left &&
				box.top < existing.bottom && box.bottom > existing.top
			) {
				overlap = true;

				break;
			}
		}

		boxesGot.push(box);

		return overlap;
	};

	for (const svg of asFragment().childNodes) {
		if (svg.nodeName === "svg") {
			for (const child of svg.childNodes) {
				if (child.nodeName === "rect") {
					const left = parseFloat((child as SVGRectElement).getAttribute("x") || "0"),
					top = parseFloat((child as SVGRectElement).getAttribute("y") || "0"),
					width = parseFloat((child as SVGRectElement).getAttribute("width") || "0"),
					height = parseFloat((child as SVGRectElement).getAttribute("height") || "0"),
					bottom = top + height,
					right = left + width,
					area = width * height,
					expectedArea = expectedAreas.get(child.textContent ?? "");

					expect(area).toEqual(expectedArea);
					expect(left).toBeGreaterThanOrEqual(0);
					expect(top).toBeGreaterThanOrEqual(0);
					expect(right).toBeLessThanOrEqual(10);
					expect(bottom).toBeLessThanOrEqual(10);

					expect(overlapsExisting({
						top,
						left,
						bottom,
						right
					})).toEqual(false);
				}
			}
		}
	}

	expect(boxesGot.length).toEqual(4);
});