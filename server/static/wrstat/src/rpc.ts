import { getCookie } from "./auth";


export type ChildFilter = {
	path: string;
	groups?: string;
	users?: string;
	types?: string;
}

export type Child = {
	name: string;
	path: string;
	count: number;
	size: number;
	atime: string;
	mtime: string;
	users: string[];
	groups: string[];
	filetypes: string[];
	has_children: boolean;
	children: Child[];
	timestamp: string;
	areas: Record<string, string[]>;
	noauth: boolean;
}

export type TreeData = {
	name: string;
	path: string;
}

export type Usage = {
	GID: number;
	UID: number;
	GIDs: number[];
	UIDs: number[];
	Name: string;
	Owner: string;
	BaseDir: string;
	UsageSize: number;
	QuotaSize: number;
	UsageInodes: number;
	QuotaInodes: number;
	Mtime: string;
	DateNoSpace: string;
	DateNoFiles: string;
	status?: string;
	percentSize?: number;
	percentInodes?: number;
}

export type SubDir = {
	SubDir: string;
	NumFiles: number;
	SizeFiles: number;
	LastModified: string;
	FileUsage: Record<number, number>;
}

export type History = {
	Date: string;
	UsageSize: number;
	QuotaSize: number;
	UsageInodes: number;
	QuotaInodes: number;
}

const endpointREST = "/rest/v1/auth",
	treeURL = endpointREST + "/tree",
	groupUsageURL = endpointREST + "/basedirs/usage/groups",
	userUsageURL = endpointREST + "/basedirs/usage/users",
	groupSubDirPathURL = endpointREST + "/basedirs/subdirs/group",
	userSubDirPathURL = endpointREST + "/basedirs/subdirs/user",
	baseDirHistoryURL = endpointREST + "/basedirs/history";

const cache = new Map<string, string>(),
	getURL = <T>(url: string, params: Record<string, unknown> = {}) => {
		return new Promise<T>((successFn, errorFn) => {
			const xh = new XMLHttpRequest(),
				urlParams: string[] = [];

			for (const [key, value] of Object.entries(params)) {
				urlParams.push(`${key}=${encodeURIComponent(value + "")}`);
			}

			if (urlParams.length) {
				url += "?" + urlParams.join("&");
			}

			const res = cache.get(url);
			if (res) {
				successFn(JSON.parse(res));

				return;
			}

			xh.open("GET", url);
			xh.setRequestHeader("Authorization", `Bearer ${getCookie("jwt")}`);
			xh.send();

			xh.addEventListener("readystatechange", () => {
				if (xh.readyState === 4) {
					if (xh.status === 200) {
						cache.set(url, xh.responseText);
						successFn(JSON.parse(xh.responseText));
					} else {
						errorFn(new Error(xh.responseText));
					}
				}
			});
		});
	}

export default {
	"getChildren": (filter: ChildFilter) => getURL<Child>(treeURL, filter),
	"getGroupUsageData": () => getURL<Usage[]>(groupUsageURL),
	"getUserUsageData": () => getURL<Usage[]>(userUsageURL),
	"getBasedirsGroupSubdirs": (id: number, basedir: string) => getURL<SubDir[]>(groupSubDirPathURL, { id, basedir }),
	"getBasedirsUserSubdirs": (id: number, basedir: string) => getURL<SubDir[]>(userSubDirPathURL, { id, basedir }),
	"getBasedirsHistory": (id: number, basedir: string) => getURL<History[]>(baseDirHistoryURL, { id, basedir })
}