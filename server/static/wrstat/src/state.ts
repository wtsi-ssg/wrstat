import { useState } from "react";

const state = new Map<string, any>(),
	isDefaultValue = <T>(v: T, def: T) => v === def || JSON.stringify(v) === JSON.stringify(def),
	restoreState = <T>(name: string, v: T) => {
		const s = state.get(name);

		if (v instanceof Array && s instanceof Array) {
			return s;
		} else if (typeof v !== typeof s || state === undefined) {
			return v;
		}

		return s;
	},
	setQueryState = () => {
		if (stateTO !== -1) {
			return;
		}

		stateTO = window.setTimeout(() => {
			const query: string[] = [];

			for (const [key, value] of state) {
				const def = setters.get(key)?.[0];
				if (def !== undefined && !isDefaultValue(value, def)) {
					query.push(`${key}=${encodeURIComponent(JSON.stringify(value))}`);
				}
			}

			const queryStr = query.join("&");

			if (queryStr !== window.location.search.slice(1)) {
				window.history.pushState(Date.now(), "", "?" + queryStr);
			}

			stateTO = -1;
		});
	},
	getStateFromQuery = () => {
		state.clear();

		for (const [key, value] of new URLSearchParams(window.location.search)) {
			state.set(key, JSON.parse(value));
		}
	},
	setters = new Map<string, [any, React.Dispatch<React.SetStateAction<any>>]>();

let stateTO = -1;

window.addEventListener("popstate", () => {
	getStateFromQuery();

	restoring = true;

	window.setTimeout(() => restoring = false, 1);

	for (const [key, [v, fn]] of setters) {
		fn(state.get(key) ?? v);
	}
});

getStateFromQuery();

export let restoring = false,
	firstRender = true;

let firstSet = -1;

export const useSavedState = <T>(name: string, v: T) => {
	const [val, setter] = useState<T>(restoreState(name, v));

	setters.set(name, [v, setter]);

	if (firstRender) {
		if (firstSet >= 0) {
			clearTimeout(firstSet);
		}

		firstSet = window.setTimeout(() => firstRender = false, 1);
	}

	return [val, (val: T) => {
		if (firstRender || restoring) {
			return;
		}

		state.set(name, val);

		setter(val);

		setQueryState();
	}] as const;
},
	clearState = () => {
		state.clear();
		setQueryState();

		for (const [v, fn] of setters.values()) {
			fn(v);
		}
	};