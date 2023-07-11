import {useEffect, useState} from "react";

export default ({min = 0, max = min + 1, minValue = min, maxValue = max, onchange, width, ticks = 5, noOverlap = true, formatter}:
	 {min?: number; max?: number; minValue?: number; maxValue?: number; ticks?: number, width: number,
		onchange: (min: number, max: number) => void, noOverlap?: boolean, formatter: (val: number) => string}) => {

	width = Math.max(width, 100)

	const [sliderMin, setSliderMin] = useState(Math.max(min, minValue)),
	[sliderMax, setSliderMax] = useState(Math.min(max, maxValue)),
	safeMin = Math.min(Math.max(min, sliderMin), Math.max(max, min)),
	safeMax = Math.max(Math.min(max, sliderMax), Math.min(min, max)),
	minX = width * (sliderMin / max),
	maxX = width * (sliderMax / max);

	useEffect(() => {
		setSliderMin(Math.max(min, minValue));
		setSliderMax(Math.min(max, maxValue));
	}, [minValue, maxValue]);

	let draggingMin = false,
	offsetLeft = 0;

	const getMinMax = (e: MouseEvent) => {
		const val = ticks * Math.round((max - min) * (e.clientX - offsetLeft) / (width * ticks));
	
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
	minAndMax = [
		<div className="minmax_minSlider" onMouseDown={e => mousedown(e, true)} style={{left: `calc(${minX}px - 0.5em)`}} />,
		<div className="minmax_maxSlider" onMouseDown={e => mousedown(e, false)} style={{left: `calc(${maxX}px - 0.5em)`}} />
	];

	if (sliderMin - min > max - sliderMax) {
		minAndMax.reverse();
	}

	return <div className="minmax" style={{width: width + "px"}}>
		<div className="minmax_min">{formatter(min)}</div>
		<div className="minmax_max">{formatter(max)}</div>
		<div className="minmax_line" />
		<div className="minmax_setLine" style={{left: minX + "px", right: (width - maxX) + "px"}} />
		<div className="minmax_minValue" style={{right: (width - minX) + "px"}}>{formatter(sliderMin)}</div>
		<div className="minmax_maxValue" style={{left: maxX + "px"}}>{formatter(sliderMax)}</div>
		{minAndMax}
	</div>
};
