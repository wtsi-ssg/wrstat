import {useEffect, useState} from "react";

const state = new Map<string, any>(),
isDefaultValue = <T>(v: T, def: T) => JSON.stringify(v) === JSON.stringify(def),
restoreState = <T>(name: string, v: T) => {
	const s = state.get(name);

	if (v instanceof Array && s instanceof Array) {
		return s;
	} else if (typeof v !== typeof s || state === undefined) {
		return v;
	}

	return s;
},
setHashState = () => {
	if (stateTO !== -1) {
		return;
	}

	stateTO = window.setTimeout(() => {
		let query: string[] = [];

		for (const [key, value] of state) {
			if (!isDefaultValue(value, setters.get(key)?.[0])) {
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

let stateTO = -1,
inited = false,
setInit = -1;

window.addEventListener("popstate", () => {
	getStateFromQuery();
	for (const [key, [v, fn]] of setters) {
		fn(state.get(key) ?? v)
	}
});

getStateFromQuery();

export const useSavedState = <T>(name: string, v: T) => {
	const [val, setter] = useState<T>(restoreState(name, v));
	setters.set(name, [v, setter]);
	
	return [val, (val: T) => {
		state.set(name, val);

		setter(val);

		setHashState();
	}] as const;
},
useEffectAfterInit = (effect: React.EffectCallback, deps?: React.DependencyList | undefined) => {
	if (!inited) {
		if (setInit !== -1) {
			clearTimeout(setInit);
		}

		setInit = window.setTimeout(() => inited = true, 10);
	}

	useEffect(() => {
		if (!inited) {
			return;
		}

		return effect();
	}, deps);
};