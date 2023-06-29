/*

#moveSlider(e: MouseEvent, dragging: HTMLDivElement) {
	const [min, max] = this.#getMinMax(e, dragging);

	this.#setSliders(min, max);
}

#dropSlider(e: MouseEvent, dragging: HTMLDivElement) {
	const [min, max] = this.#getMinMax(e, dragging);
	if (this.dispatchEvent(new CustomEvent("change", {"cancelable": true, "detail": {min, max}}))) {
		this.#minValue = min;
		this.#maxValue = max;
	}
	
	this.#setSliders(this.#minValue, this.#maxValue);
}
*/

export default ({min = 0, max, minValue = 0, maxValue = max, onchange, width, ticks = 5, noOverlap = true}:
	 {min?: number; max: number; minValue?: number; maxValue?: number; ticks?: number, width: number,
		onchange: (min: number, max: number) => void, noOverlap?: boolean}) => {
	
	let draggingMin = false;

	const getMinMax = (e: MouseEvent) => {
		const minX = 0,
		val = ticks * Math.round((max - min) * (e.clientX - minX) / (width * ticks));
	
		let [amin, amax] = getSafeMinMax();
	
		if (draggingMin) {
			amin = Math.min(Math.max(val, min), max, amax - +noOverlap * ticks);
		} else {
			amax = Math.max(Math.min(val, max), min, amin + +noOverlap * ticks);
		}
	
		return [amin, amax];
	},
	getSafeMinMax = () => {
		const amin = Math.min(Math.max(min, minValue), Math.max(max, min)),
			  amax = Math.max(Math.min(max, maxValue), Math.min(min, max));
	
		return [amin, amax];
	},
	dragMin = () => {},
	dragMax = () => {};

	return <div className="minmax">
		<div className="minmax_min">{min}</div>
		<div className="minmax_max">{max}</div>
		<div className="minmax_line" />
		<div className="minmax_setline" />
		<div className="minmax_minvalue" style={{left: 0}}>{minValue}</div>
		<div className="minmax_maxvalue" style={{right: 0}}>{maxValue}</div>
		<div className="minmax_minslider" onMouseDown={dragMin} />
		<div className="minmax_maxslider" onMouseDown={dragMax} />
	</div>
};

/*

		let dragging: HTMLDivElement | null = null;
		const [startDrag] = mouseDragEvent(
			0,
			(e: MouseEvent) => this.#moveSlider(e, dragging!),
			(e: MouseEvent) => {
				this.#dropSlider(e, dragging!);
				dragging = null;
			}
		      ),
		      dragSlider = (e: MouseEvent) => {
			if (e.button === 0) {
				e.preventDefault();
				amendNode(this.#shadow, dragging = e.target as HTMLDivElement);
				startDrag();
			}
		      }

		amendNode(this.#shadow = this.attachShadow({"mode": "closed"}), [
			this.#minText = div({"id": min}),
			this.#maxText = div({"id": max}),
			div({"id": line}),
			this.#selectedLine = div({"id": selLine}),
			this.#minValueText = div({"id": minValue}),
			this.#maxValueText = div({"id": maxValue}),
			this.#minSlider = div({"id": minSlider, "onmousedown": dragSlider}),
			this.#maxSlider = div({"id": maxSlider, "onmousedown": dragSlider})
		]).adoptedStyleSheets = minMaxStyles;

		this.#build();
	}


	#build() {
		clearNode(this.#minText, this.#min + "");
		clearNode(this.#maxText, (this.#max < this.#min ? this.#min : this.#max) + "");

		const [min, max] = this.#getSafeMinMax();

		amendNode(this.#shadow, min - this.#min > this.#max - max ? this.#minSlider : this.#maxSlider);

		this.#setSliders(min, max);
	}

	#setSliders(min: number, max: number) {
		const width = this.clientWidth,
		      minX = width * (min / this.#max),
		      maxX = width * (max / this.#max);

		clearNode(this.#minValueText, {"style": {"right": `${width - minX}px`}}, min + "");
		clearNode(this.#maxValueText, {"style": {"left": `${maxX}px`}}, max + "");
		amendNode(this.#minSlider, {"style": {"left": `calc(${minX}px - 0.5em)`}});
		amendNode(this.#maxSlider, {"style": {"left": `calc(${maxX}px - 0.5em)`}});
		amendNode(this.#selectedLine, {"style": {"left": minX + "px", "right": (width - maxX) + "px"}});
	}
}
*/