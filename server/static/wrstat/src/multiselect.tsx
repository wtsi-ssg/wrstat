import {useRef, useState} from "react";

export default ({id, list, onchange}: {id: string; list: string[]; onchange: (list: string[]) => void}) => {
	const [selected, setSelected] = useState(new Set<string>()),
	filterRef = useRef<HTMLInputElement>(null),
	[filter, setFilter] = useState(""),
	filteredList = list.filter(e => e.toLowerCase().includes(filter.toLowerCase()));

	return <div className="multiInput">
		<ul>
			<li><button id={id} onClick={() => filterRef.current?.focus()}>+</button></li>
			{Array.from(selected).map(e => <li onClick={() => {
				selected.delete(e);
				setSelected(new Set(selected));
				onchange(Array.from(selected));
			}}>{e}</li>)}
		</ul>
		<div>
			<div>
				<input ref={filterRef} value={filter} onChange={e => setFilter(e.target.value)} />
				<ul tabIndex={-1}>
					{
						filteredList.map(e => <li>
							<label><input type="checkbox" checked={selected.has(e)} onChange={() => {
								if (selected.has(e)) {
									selected.delete(e);
								} else {
									selected.add(e);
								}

								setSelected(new Set(selected));
								onchange(Array.from(selected));
							}} />{e}</label>
						</li>)
					}
				</ul>
			</div>
		</div>
	</div>
};