import { type KeyboardEvent, useState, useLayoutEffect } from "react";

type MinMaxParams = {
	min?: number;
	max?: number;
	minValue?: number;
	maxValue?: number;
	ticks?: number;
	width: number;
	onchange: (min: number, max: number) => void;
	noOverlap?: boolean;
	formatter: (val: number) => string;
	label?: string;
}

const MinmaxComponent = ({
	min = 0,
	max = min + 1,
	minValue = min,
	maxValue = max,
	onchange,
	width,
	ticks = 5,
	noOverlap = true,
	formatter,
	label = ""
}: MinMaxParams) => {

	width = Math.max(width, 100)

	const [sliderMin, setSliderMin] = useState(Math.max(min, minValue)),
		[sliderMax, setSliderMax] = useState(Math.min(max, maxValue)),
		safeMin = Math.min(Math.max(min, sliderMin), Math.max(max, min)),
		safeMax = Math.max(Math.min(max, sliderMax), Math.min(min, max)),
		minX = width * (sliderMin / max),
		maxX = width * (sliderMax / max);

	if (minValue < min) {
		minValue = min;
	}

	if (maxValue > max) {
		maxValue = max;
	}

	useLayoutEffect(() => {
		setSliderMin(Math.max(min, minValue));
		setSliderMax(Math.min(max, maxValue));
	}, [minValue, maxValue]);

	let draggingMin = false,
		offsetLeft = 0;

	const getMinMax = (e: MouseEvent) => getSafeMinMax(ticks * Math.round((max - min) * (e.clientX - offsetLeft) / (width * ticks))),
		getSafeMinMax = (val: number) => {
			let amin = safeMin,
				amax = safeMax;

			if (draggingMin) {
				amin = Math.min(Math.max(val, min), max, amax - +noOverlap * ticks);
			} else {
				amax = Math.max(Math.min(val, max), min, amin + +noOverlap * ticks);
			}

			return [amin, amax];
		},
		mousedown = (e: React.MouseEvent<HTMLDivElement, MouseEvent>, which: boolean) => {
			if (e.button !== 0) {
				return;
			}

			offsetLeft = ((e.target as HTMLDivElement).offsetParent as HTMLDivElement)?.offsetLeft ?? 0;

			draggingMin = which;
			window.addEventListener("mousemove", mousemove);
			window.addEventListener("mouseup", mouseup);
		},
		mousemove = (e: MouseEvent) => {
			const [min, max] = getMinMax(e);

			setSliderMin(min);
			setSliderMax(max);
		},
		mouseup = (e: MouseEvent) => {
			if (e.button !== 0) {
				return;
			}

			window.removeEventListener("mousemove", mousemove);
			window.removeEventListener("mouseup", mouseup);

			const [min, max] = getMinMax(e);

			onchange(min, max);
			setSliderMin(min);
			setSliderMax(max);
		},
		onkeyup = (e: KeyboardEvent) => {
			if (e.key !== "ArrowLeft" && e.key !== "ArrowRight") {
				return;
			}

			onchange(sliderMin, sliderMax);
		},
		onkeydown = (e: KeyboardEvent) => {
			if (e.key !== "ArrowLeft" && e.key !== "ArrowRight" || e.altKey || e.metaKey || e.ctrlKey) {
				return;
			}

			e.preventDefault();

			draggingMin = (e.target as HTMLElement)?.className.includes("minmax_minSlider");

			const [smin, smax] = getSafeMinMax(ticks * Math.round((max - min) * (((draggingMin ? minX : maxX) + (e.key === "ArrowLeft" ? -1 : 1)) - offsetLeft) / (width * ticks)));

			setSliderMin(smin);
			setSliderMax(smax);

		};

	return <div className="minmax" style={{ width: width + "px" }}>
		<div className="minmax_min">{formatter(min)}</div>
		<div className="minmax_max">{formatter(max)}</div>
		<div className="minmax_line" />
		<div className="minmax_setLine" style={{ left: minX + "px", right: (width - maxX) + "px" }} />
		<div className="minmax_minValue" style={{ right: (width - minX) + "px" }}>{formatter(sliderMin)}</div>
		<div className="minmax_maxValue" style={{ left: maxX + "px" }}>{formatter(sliderMax)}</div>
		<div className="minmax_minSlider" role="slider" aria-label={`Minimum ${label}`} aria-valuemin={min} aria-valuetext={formatter(minValue)} aria-valuenow={minValue} aria-valuemax={maxValue} tabIndex={0} onKeyDown={onkeydown} onKeyUp={onkeyup} onMouseDown={e => mousedown(e, true)} style={{ left: `calc(${minX}px - 0.5em)`, "zIndex": sliderMin - min > max - sliderMax ? 2 : 1 }} />
		<div className="minmax_maxSlider" role="slider" aria-label={`Maximum ${label}`} aria-valuemin={minValue} aria-valuetext={formatter(maxValue)} aria-valuenow={maxValue} aria-valuemax={max} tabIndex={0} onKeyDown={onkeydown} onKeyUp={onkeyup} onMouseDown={e => mousedown(e, false)} style={{ left: `calc(${maxX}px - 0.5em)`, "zIndex": sliderMin - min > max - sliderMax ? 1 : 2 }} />
	</div>
};

export default MinmaxComponent;