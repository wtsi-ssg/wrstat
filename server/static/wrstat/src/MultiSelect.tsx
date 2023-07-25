import type { KeyboardEvent } from "react";
import { useRef, useState } from "react";

type MultiSelectParams = {
	id: string;
	disabled?: boolean;
	list: readonly string[];
	selected: readonly string[];
	listener?: (cb: Listener) => void;
	onchange: (list: string[], deleted: string | null) => void;
}

type ItemParams = {
	item: string;
	selectedSet: Set<string>;
	onchange: (list: string[], deleted: string | null) => (void | boolean);
}

type SelectItemParams = ItemParams & {
	keypress: (e: KeyboardEvent) => void;
	disabled: boolean;
}

export type Listener = (values: string[], deleted: boolean) => void;

const SelectItem = ({ item, selectedSet, disabled, onchange, keypress }: SelectItemParams) => <li>
	<label>
		<input tabIndex={-1} type="checkbox" disabled={disabled} checked={selectedSet.has(item)} onChange={() => {
			let deleted: null | string = null;

			if (selectedSet.has(item)) {
				selectedSet.delete(item);
				deleted = item;
			} else {
				selectedSet.add(item);
			}

			const selected = Array.from(selectedSet);

			onchange(selected, deleted);
		}} onKeyDown={keypress} />{item}</label>
</li>,
	RemoveItem = ({ item, selectedSet, onchange }: ItemParams) => <li>
		<button tabIndex={-1} title={`Deselect ${item}`} onClick={() => {
			selectedSet.delete(item);

			const selected = Array.from(selectedSet);

			onchange(selected, item);
		}}>{item}</button>
	</li>,
	MultiSelectComponent = ({
		id,
		list,
		selected,
		disabled = false,
		listener,
		onchange
	}: MultiSelectParams) => {

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

			onchange(newSelected, null);
		});

		return <div className="multiSelect" id={`multi_${id}`}>
			<ul>
				<li>
					<button
						id={id}
						aria-haspopup="listbox"
						aria-label="Select/Deselect"
						disabled={disabled}
						onClick={() => filterRef.current?.focus()}
					>+</button>
				</li>
				{disabled ? <></> : Array.from(selected).map(item => <RemoveItem
					key={`ms_${id}_s_${item}`}
					item={item}
					selectedSet={selectedSet}
					onchange={onchange}
				/>)}
			</ul>
			<div>
				<div>
					<input
						ref={filterRef}
						placeholder="Filter"
						disabled={disabled}
						aria-label="Filter List"
						tabIndex={-1}
						value={filter}
						onKeyDown={e => {
							if (e.key === "Escape") {
								(e.target as HTMLInputElement).blur();
							} else if (e.key === "Enter") {
								if (filteredList.length === 1) {
									selectedSet.add(filteredList[0]);

									const selected = Array.from(selectedSet);

									onchange(selected, null);
								}
							} else if (e.key === "Tab") {
								e.preventDefault();

								const input = (e.target as HTMLElement)?.nextElementSibling?.[e.shiftKey ? "lastElementChild" : "firstElementChild"]?.firstElementChild?.firstElementChild;

								if (input instanceof HTMLInputElement) {
									input.focus();
								}
							}
						}}
						onChange={e => setFilter(e.target.value)}
					/>
					<ul tabIndex={-1} onKeyDown={e => {
						if (e.key === "Escape") {
							(e.target as HTMLInputElement).blur();
						}
					}}>
						{
							filteredList.map(item => <SelectItem
								key={`ms_${id}_${item}`}
								item={item}
								disabled={disabled}
								selectedSet={selectedSet}
								onchange={onchange}
								keypress={keypress}
							/>)
						}
					</ul>
				</div>
			</div>
		</div>
	};

export default MultiSelectComponent;