import type { Child, History, Usage } from './rpc';
import type { Entry } from './treemap';
import { useEffect, useState } from "react"
import { formatBytes, formatLargeNumber, formatNumber } from './format';
import HistoryGraph from './history';
import MultiSelect from './multiselect';
import rpc from "./rpc";
import { restoring, useSavedState } from './state';
import SubDirs from './subdirs';
import type { Filter } from './table';
import TreeDetails from "./treedetails";
import Treemap from "./treemap";

const colours = [
	"#d73027",
	"#f46c43",
	"#fdaf61",
	"#fedf8b",
	"#ffffbf",
	"#d9ef8b",
	"#a6d96a",
	"#66bd63",
	"#1a9850",
	"#fff"
] as const,
	now = +Date.now(),
	colourFromAge = (lm: number) => {
		const diff = now - lm;

		const day = 24 * 60 * 60 * 1000;

		if (diff > 2 * 365 * day) {
			return colours[0];
		} else if (diff > 365 * day) {
			return colours[1];
		} else if (diff > 10 * 30 * day) {
			return colours[2];
		} else if (diff > 8 * 30 * day) {
			return colours[3];
		} else if (diff > 6 * 30 * day) {
			return colours[4];
		} else if (diff > 3 * 30 * day) {
			return colours[5];
		} else if (diff > 2 * 30 * day) {
			return colours[6];
		} else if (diff > 30 * day) {
			return colours[7];
		}
		return colours[8];
	},
	makeBreadcrumb = (path: string, part: string, setPath: (path: string) => void) => {
		return <li><button onClick={() => setPath(path)}>{part}</button></li>;
	},
	makeBreadcrumbs = (path: string, setPath: (path: string) => void) => {
		let last = 0;

		const breadcrumbs = [
			makeBreadcrumb("/", "Root", setPath)
		];

		while (true) {
			const pos = path.indexOf("/", last + 1);

			if (pos === -1) {
				break;
			}

			breadcrumbs.push(makeBreadcrumb(path.slice(0, pos), path.slice(last + 1, pos), setPath));

			last = pos;
		}

		breadcrumbs.push(<span>{path.slice(last + 1) || "/"}</span>);

		return breadcrumbs;
	},
	determineTreeWidth = () => {
		const width = window.innerWidth;

		return width - 40;
	},
	makeFilter = (path: string, filter: Filter<Usage>, filetypes: string[], users: Map<number, string>, groups: Map<number, string>) => {
		return {
			path,
			"users": (filter.UID as null | number[])?.map(uid => users.get(uid) ?? -1).join(",") ?? "",
			"groups": (filter.GID as null | number[])?.map(gid => groups.get(gid) ?? -1).join(",") ?? "",
			"types": filetypes.join(",")
		};
	},
	fileTypes = ["other", "temp", "vcf", "vcf.gz", "bcf", "sam", "bam",
		"cram", "fasta", "fastq", "fastq.gz", "ped/bed", "compressed", "text",
		"log", "dir"] as const,
	timesSinceAccess = [
		["> 0 days", 0],
		["> 1 month", 30],
		["> 2 months", 60],
		["> 3 months", 120],
		["> 6 months", 180],
		["> 8 months", 240],
		["> 10 months", 300],
		["> 1 year", 365],
		["> 2 years", 730]
	] as const;

let first = true;

export default ({ id, path, isUser, filter, users, groups }: { id: number, path: string; isUser: boolean; filter: Filter<Usage>, users: Map<number, string>, groups: Map<number, string> }) => {
	const [treePath, setTreePath] = useSavedState("treePath", "/"),
		[treeMapData, setTreeMapData] = useState<Entry[] | null>(null),
		[breadcrumbs, setBreadcrumbs] = useState<JSX.Element[]>([]),
		[childDetails, setChildDetails] = useState<Child | null>(null),
		[dirDetails, setDirDetails] = useState<Child | null>(childDetails),
		[useMTime, setUseMTime] = useSavedState("useMTime", false),
		[useCount, setUseCount] = useSavedState("useCount", false),
		[treeWidth, setTreeWidth] = useState(determineTreeWidth()),
		[filterFileTypes, setFilterFileTypes] = useState<string[]>([]),
		[sinceLastAccess, setSinceLastAccess] = useSavedState("sinceLastAccess", 0),
		[inodeHistory, setInodeHistory] = useSavedState("inodeHistory", false),
		[hasAuth, setHasAuth] = useState(true),
		[history, setHistory] = useState<History[]>([]);

	useEffect(() => window.addEventListener("resize", () => setTreeWidth(determineTreeWidth())), []);

	useEffect(() => {
		if (restoring || first) {
			first = false;

			return;
		}

		setTreePath(path || "/")
	}, [path]);

	useEffect(() => {
		if (id === -1 || path === "") {
			return;
		}

		rpc.getBasedirsHistory(id, path).then(setHistory);
	}, [id, path]);

	useEffect(() => {
		rpc.getChildren(makeFilter(treePath, filter, filterFileTypes, users, groups))
			.then(children => {
				const entries: Entry[] = [],
					since = new Date(children.timestamp).valueOf() - sinceLastAccess * 86_400_000;

				setHasAuth(!children.noauth);

				for (const child of children.children ?? []) {
					if (new Date(child.atime).valueOf() > since) {
						continue;
					}

					entries.push({
						key: btoa(child.path),
						name: child.name,
						value: useCount ? child.count : child.size,
						backgroundColour: colourFromAge(+(new Date(useMTime ? child.mtime : child.atime))),
						onclick: child.has_children && !child.noauth ? () => setTreePath(child.path) : undefined,
						onmouseover: () => setChildDetails(child),
						noauth: child.noauth
					});
				}

				entries.sort((a, b) => b.value - a.value);

				setTreeMapData(entries);
				setChildDetails(children);
				setDirDetails(children);
			});

		setBreadcrumbs(makeBreadcrumbs(treePath, setTreePath));
	}, [treePath, useMTime, useCount, filterFileTypes, sinceLastAccess, JSON.stringify(filter)]);

	return <>
		<div className="treeFilter">
			<label htmlFor="aTime">Use ATime</label><input type="radio" id="aTime" checked={!useMTime} onChange={() => setUseMTime(false)} />
			<label htmlFor="mTime">Use MTime</label><input type="radio" id="mTime" checked={useMTime} onChange={() => setUseMTime(true)} />
			<br />
			<label htmlFor="useSize">Use Size</label><input type="radio" id="useSize" checked={!useCount} onChange={() => setUseCount(false)} />
			<label htmlFor="useCount">Use Count</label><input type="radio" id="useCount" checked={useCount} onChange={() => setUseCount(true)} />
			<br />
			<label htmlFor="filetypes">File Types: </label><MultiSelect id="filetypes" list={fileTypes} onchange={setFilterFileTypes} />
			<br />
			<label htmlFor="sinceAccess">Time Since Access</label>
			<select onChange={e => setSinceLastAccess(parseInt(e.target.value) ?? 0)}>
				{timesSinceAccess.map(([l, t]) => <option selected={sinceLastAccess === t} value={t}>{l}</option>)}
			</select>
		</div>
		<ul id="treeBreadcrumbs">{breadcrumbs}</ul>
		<Treemap table={treeMapData} width={treeWidth} height={500} noAuth={!hasAuth} onmouseout={() => setChildDetails(dirDetails)} />
		<TreeDetails details={childDetails} />
		<table id="treeKey">
			<caption>
				<span>Colour Key</span>
				{useMTime ? "Least" : "Greatest"} time since a file nested within the directory was {useMTime ? "modified" : "accessed"}:
			</caption>
			<tbody>
				<tr>
					<td className="age_2years">&gt; 2 years</td>
					<td className="age_1year">&gt; 1 year</td>
					<td className="age_10months">&gt; 10 months</td>
					<td className="age_8months">&gt; 8 months</td>
					<td className="age_6months">&gt; 6 months</td>
					<td className="age_3months">&gt; 3 months</td>
					<td className="age_2months">&gt; 2 months</td>
					<td className="age_1month">&gt; 1 month</td>
					<td className="age_1week">&lt; 1 month</td>
				</tr>
			</tbody>
		</table>
		<SubDirs id={id} path={path} isUser={isUser} setPath={setTreePath} />
		{
			history.length ? <>
				<label htmlFor="sizeHistory">Size History</label><input type="radio" id="sizeHistory" checked={!inodeHistory} onChange={() => setInodeHistory(false)} />
				<label htmlFor="countHistory">Count History</label><input type="radio" id="countHistory" checked={inodeHistory} onChange={() => setInodeHistory(true)} />
			</> : <></>
		}
		<HistoryGraph history={history.map(h => ({ Date: h.Date, Usage: inodeHistory ? h.UsageInodes : h.UsageSize, Quota: inodeHistory ? h.QuotaInodes : h.QuotaSize }))} width={960} height={500} yFormatter={inodeHistory ? formatLargeNumber : formatBytes} secondaryFormatter={inodeHistory ? formatNumber : (num: number) => formatNumber(num) + " Bytes"} yRounder={inodeHistory ? (maxAmount: number) => {
			const order = Math.pow(10, Math.max(Math.floor(Math.log10(maxAmount)), 1));

			return maxAmount = order * Math.ceil(maxAmount / order)
		} : (maxAmount: number) => 100 * Math.pow(2, Math.ceil(Math.log2(maxAmount / 100)))} />
	</>
}
