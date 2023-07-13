import { useEffect, useRef, useState } from "react";
import { useSavedState } from "./state";

const MultiselectComponent = ({ id, list, disabled = false, onchange }: { id: string; disabled?: boolean; list: readonly string[]; onchange: (list: string[]) => void }) => {
	const [selected, setSelected] = useSavedState<string[]>(id + "Multi", []),
		filterRef = useRef<HTMLInputElement>(null),
		[filter, setFilter] = useState(""),
		selectedSet = new Set(selected),
		filteredList = list.filter(e => e.toLowerCase().includes(filter.toLowerCase()));

	useEffect(() => onchange(Array.from(selected)), []);

	return <div className="multiInput">
		<ul>
			<li><button id={id} disabled={disabled} onClick={() => filterRef.current?.focus()}>+</button></li>
			{(disabled ? [] : Array.from(selected)).map(e => <li onClick={() => {
				selectedSet.delete(e);

				const selected = Array.from(selectedSet);

				setSelected(selected);
				onchange(selected);
			}}>{e}</li>)}
		</ul>
		<div>
			<div>
				<input ref={filterRef} value={filter} onChange={e => setFilter(e.target.value)} />
				<ul tabIndex={-1}>
					{
						filteredList.map(e => <li>
							<label><input type="checkbox" checked={selectedSet.has(e)} onChange={() => {
								if (selectedSet.has(e)) {
									selectedSet.delete(e);
								} else {
									selectedSet.add(e);
								}

								const selected = Array.from(selectedSet);

								setSelected(selected);
								onchange(selected);
							}} />{e}</label>
						</li>)
					}
				</ul>
			</div>
		</div>
	</div>
};

export default MultiselectComponent;