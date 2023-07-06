import {useEffect, useState} from "react";

const state: Record<string, any> = {},
safeValues = (_: string, v: any) => {
	if (v === null || v === undefined) {
		return undefined;
	} else if (v === Infinity) {
		return "∞";
	} else if (v === -Infinity) {
		return "-∞";
	} else if (v instanceof Array && v.length === 0) {
		return undefined;
	}

	return v;
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
		const hash = encodeURI(JSON.stringify(state, safeValues));

		if (hash) {
			window.location.hash = "#" + hash;
		} else {
			window.location.hash = "";
		}
		
		stateTO = -1;
	});
};

let stateTO = -1,
inited = false,
setInit = -1;

try {
	const v = JSON.parse(decodeURI(window.location.hash.slice(1)), (_: string, v: string) => {
		if (v === "∞") {
			return Infinity;
		} else if (v === "-∞") {
			return -Infinity;
		}

		return v;
	});

	if (v instanceof Object) {
		Object.assign(state, v);
	}
} catch {}

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