import useURLState from "./State";
import { useEffect, useRef, useState } from "react";

export type Listener = (values: string[], deleted: boolean) => void;

const MultiselectComponent = ({ id, list, disabled = false, selected, setSelected }: {
    id: string; disabled?: boolean; list: readonly string[];
    selected: string[];
    setSelected: any
}) => {
    // let [selected, setSelected] = useURLState<string[]>(id + "Multi", []);

    // useEffect(() => { onchange(Array.from(selected), null) }, [JSON.stringify(selected)]);

    // if (selectedList) {
    //     selected = Array.from(new Set(selected.concat(selectedList)));
    // }

    const filterRef = useRef<HTMLInputElement>(null),
        [filter, setFilter] = useState(""),
        selectedSet = new Set(selected),
        filteredList = list.filter(e => e.toLowerCase().includes(filter.toLowerCase()));

    // listener?.((values: string[], deleted: boolean) => {
    //     const valueSet = new Set(selected);

    //     if (deleted) {
    //         for (const v of values) {
    //             valueSet.delete(v);
    //         }
    //     } else {
    //         for (const v of values) {
    //             valueSet.add(v);
    //         }
    //     }

    //     const newSelected = Array.from(valueSet);

    //     if (onchange(newSelected, null) !== false) {
    //         setSelected(newSelected);
    //     }
    // });

    return <div className="multiInput">
        <ul>
            <li><button id={id} disabled={disabled} onClick={() => filterRef.current?.focus()}>+</button></li>
            {(disabled ? [] : Array.from(selected)).map(e => <li onClick={() => {
                selectedSet.delete(e);

                const selected = Array.from(selectedSet);

                // if (onchange(selected, e) !== false) {
                setSelected(selected);
                // }
            }}>{e}</li>)}
        </ul>
        <div>
            <div>
                <input ref={filterRef} value={filter} onKeyDown={e => {
                    if (e.key === "Escape") {
                        (e.target as HTMLInputElement).blur();
                    } else if (e.key === "Enter") {
                        if (filteredList.length === 1) {
                            selectedSet.add(filteredList[0]);

                            const selected = Array.from(selectedSet);

                            // if (onchange(selected, null) !== false) {
                            setSelected(selected);
                            // }
                        }
                    }
                }} onChange={e => setFilter(e.target.value)} />
                <ul tabIndex={-1}>
                    {
                        filteredList.map(e => <li>
                            <label><input type="checkbox" checked={selectedSet.has(e)} onChange={() => {
                                let deleted: null | string = null;

                                if (selectedSet.has(e)) {
                                    selectedSet.delete(e);
                                    deleted = e;
                                } else {
                                    selectedSet.add(e);
                                }

                                const selected = Array.from(selectedSet);

                                // if (onchange(selected, deleted) !== false) {
                                setSelected(selected);
                                // }
                            }} />{e}</label>
                        </li>)
                    }
                </ul>
            </div>
        </div>
    </div>
};

export default MultiselectComponent;