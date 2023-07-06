import type {Child, History, Usage} from './rpc';
import type {Entry} from './treemap';
import {useEffect, useState} from "react"
import HistoryGraph from './history';
import MultiSelect from './multiselect';
import rpc from "./rpc";
import {useSavedState, useEffectAfterInit} from './state';
import SubDirs from './subdirs';
import type {Filter} from './table';
import TreeDetails from "./treedetails";
import Treemap from "./treemap";

const colours = [
	"rgba(215, 48, 39, 0.75)",
	"rgba(244, 109, 67, 0.75)",
	"rgba(253, 174, 97, 0.75)",
	"rgba(254, 224, 139, 0.75)",
	"rgba(255, 255, 191, 0.75)",
	"rgba(217, 239, 139, 0.75)",
	"rgba(166, 217, 106, 0.75)",
	"rgba(102, 189, 99, 0.75)",
	"rgba(26, 152, 80, 0.75)",
	"rgba(255, 255, 255, 0.75)"
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
		const pos = path.indexOf("/", last+1);

		if (pos === -1) {
			break;
		}

		breadcrumbs.push(makeBreadcrumb(path.slice(0, pos), path.slice(last+1, pos), setPath));	

		last = pos;
	}

	breadcrumbs.push(<span>{path.slice(last+1) || "/"}</span>);

	return breadcrumbs;
},
widthStopMin = 400,
widthStopMax = 960,
determineTreeWidth = () => {
	const width = window.innerWidth;

	let mul = 1;

	if (width > widthStopMax) {
		mul = 0.6;
	} else if (width > widthStopMin) {
		mul = 1 - 0.4 * (width - widthStopMin) / (widthStopMax - widthStopMin);
	}

	return width * mul;
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

export default ({id, path, isUser, history, filter, users, groups}: {id: number, path: string; isUser: boolean; history: History[], filter: Filter<Usage>, users: Map<number, string>, groups: Map<number, string>}) => {
	const [treePath, setTreePath] = useSavedState("treePath", path || "/"),
	[treeMapData, setTreeMapData] = useState<Entry[] | null>(null),
	[breadcrumbs, setBreadcrumbs] = useState<JSX.Element[]>([]),
	[childDetails, setChildDetails] = useState<Child | null>(null),
	[dirDetails, setDirDetails] = useState<Child | null>(childDetails),
	[useMTime, setUseMTime] = useSavedState("useMTime", false),
	[useCount, setUseCount] = useSavedState("useCount", false),
	[treeWidth, setTreeWidth] = useState(determineTreeWidth()),
	[filterFileTypes, setFilterFileTypes] = useSavedState<string[]>("fileTypes", []),
	[sinceLastAccess, setSinceLastAccess] = useSavedState("sinceLastAccess", 0)

	useEffect(() => window.addEventListener("resize", () => setTreeWidth(determineTreeWidth())), []);

	useEffectAfterInit(() => setTreePath(path || "/"), [path]);

	useEffect(() => {
		rpc.getChildren(makeFilter(treePath, filter, filterFileTypes, users, groups))
		.then(children => {
			const entries: Entry[] = [],
			since = new Date(children.timestamp).valueOf() - sinceLastAccess * 86_400_000;

			for (const child of children.children ?? []) {
				if (new Date(child.atime).valueOf() > since) {
					continue;
				}

				entries.push({
					name: child.name,
					value: useCount ? child.count : child.size,
					backgroundColour: colourFromAge(+(new Date(useMTime ? child.mtime : child.atime))),
					onclick: child.has_children ? () => setTreePath(child.path) : undefined,
					onmouseover: () => setChildDetails(child)
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
		<Treemap table={treeMapData} width={treeWidth} height={500} onmouseout={() => setChildDetails(dirDetails)} />
		<TreeDetails details={childDetails} />
		<table id="treeKey">
			<caption>
				<span>Colour Key</span>
				Greatest time since a file nested within the directory was {useMTime ? "modified" : "accessed"}:
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
		<HistoryGraph history={history} width={960} height={500} />
	</>
}