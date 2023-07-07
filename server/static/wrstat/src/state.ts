import {useEffect, useState} from "react";

const state: Record<string, any> = {},
isSafeValue = (v: any) => {
	if (v === null || v === undefined) {
		return false;
	} else if (v === Infinity || v === -Infinity || v === 0) {
		return false;
	} else if (v instanceof Array && v.length === 0) {
		return false;
	}

	return true;
},
restoreState = <T>(name: string, v: T) => {
	const s = state[name];

	if (v instanceof Set && s instanceof Array) {
		return new Set(s);
	} else if (v instanceof Array && s instanceof Array) {
		return s;
	} else if (typeof v !== typeof state[name] || state === undefined) {
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

		for (const [key, value] of Object.entries(state)) {
			if (isSafeValue(value)) {
				query.push(`${key}=${encodeURIComponent(JSON.stringify(value))}`);
			}
		}

		const queryStr = query.join("&");

		if (queryStr !== window.location.search) {
			window.history.pushState(Date.now(), "", "?" + queryStr);
		}
		
		stateTO = -1;
	});
};

let stateTO = -1,
inited = false,
setInit = -1;

for (const [key, value] of new URLSearchParams(window.location.search)) {
	state[key] = JSON.parse(value);
}

export const useSavedState = <T>(name: string, v: T) => {

	const [val, setter] = useState<T>(restoreState(name, v));
	
	return [val, (val: T) => {
		state[name] = val instanceof Set ? Array.from(val) : val

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