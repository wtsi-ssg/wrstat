import { useEffect, useRef, useState, type KeyboardEvent } from "react";
import { useSavedState } from "./state";

type MultiSelectParams = {
	id: string;
	disabled?: boolean;
	list: readonly string[];
	selectedList?: readonly string[];
	listener?: (cb: Listener) => void;
	onchange: (list: string[], deleted: string | null) => (void | boolean);
}

export type Listener = (values: string[], deleted: boolean) => void;

const MultiSelectComponent = ({
	id,
	list,
	disabled = false,
	selectedList,
	listener,
	onchange
}: MultiSelectParams) => {
	let [selected, setSelected] = useSavedState<string[]>(id + "Multi", []);

	useEffect(() => { onchange(Array.from(selected), null) }, [JSON.stringify(selected)]);

	if (selectedList) {
		selected = Array.from(new Set(selected.concat(selectedList)));
	}

	const filterRef = useRef<HTMLInputElement>(null),
		[filter, setFilter] = useState(""),
		selectedSet = new Set(selected),
		filteredList = list.filter(e => e.toLowerCase().includes(filter.toLowerCase())),
		keypress = (e: KeyboardEvent) => {
			if (e.key === "Tab") {
				const elmPP = (e.target as HTMLInputElement)?.parentElement?.parentElement,
					elm = elmPP?.[e.shiftKey ? "previousElementSibling" : "nextElementSibling"]?.firstElementChild?.firstElementChild ??
						elmPP?.parentElement?.parentElement?.firstElementChild;

				if (elm instanceof HTMLInputElement) {
					elm.focus();

					e.preventDefault();
				}
			}
		};

	listener?.((values: string[], deleted: boolean) => {
		const valueSet = new Set(selected);

		if (deleted) {
			for (const v of values) {
				valueSet.delete(v);
			}
		} else {
			for (const v of values) {
				valueSet.add(v);
			}
		}

		const newSelected = Array.from(valueSet);

		if (onchange(newSelected, null) !== false) {
			setSelected(newSelected);
		}
	});

	return <div className="multiInput" id={`multi_${id}`}>
		<ul>
			<li><button id={id} disabled={disabled} onClick={() => filterRef.current?.focus()}>+</button></li>
			{(disabled ? [] : Array.from(selected)).map(e => <li><button onClick={() => {
				selectedSet.delete(e);

				const selected = Array.from(selectedSet);

				if (onchange(selected, e) !== false) {
					setSelected(selected);
				}
			}}>{e}</button></li>)}
		</ul>
		<div>
			<div>
				<input ref={filterRef} tabIndex={-1} value={filter} onKeyDown={e => {
					if (e.key === "Escape") {
						(e.target as HTMLInputElement).blur();
					} else if (e.key === "Enter") {
						if (filteredList.length === 1) {
							selectedSet.add(filteredList[0]);

							const selected = Array.from(selectedSet);

							if (onchange(selected, null) !== false) {
								setSelected(selected);
							}
						}
					} else if (e.key === "Tab") {
						e.preventDefault();

						const input = (e.target as HTMLElement)?.nextElementSibling?.[e.shiftKey ? "lastElementChild" : "firstElementChild"]?.firstElementChild?.firstElementChild;

						if (input instanceof HTMLInputElement) {
							input.focus();
						}
					}
				}} onChange={e => setFilter(e.target.value)} />
				<ul tabIndex={-1} onKeyDown={e => {
					if (e.key === "Escape") {
						(e.target as HTMLInputElement).blur();
					}
				}}>
					{
						filteredList.map(e => <li>
							<label><input tabIndex={-1} type="checkbox" checked={selectedSet.has(e)} onChange={() => {
								let deleted: null | string = null;

								if (selectedSet.has(e)) {
									selectedSet.delete(e);
									deleted = e;
								} else {
									selectedSet.add(e);
								}

								const selected = Array.from(selectedSet);

								if (onchange(selected, deleted) !== false) {
									setSelected(selected);
								}
							}} onKeyDown={keypress} />{e}</label>
						</li>)
					}
				</ul>
			</div>
		</div>
	</div>
};

export default MultiSelectComponent;