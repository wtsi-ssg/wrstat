import type { Child, Usage } from './rpc';
import type { Filter } from './Table';
import type { Entry } from './Treemap';
import { useEffect, useState } from "react"
import MultiSelect from './MultiSelect';
import TreeDetails from "./TreeDetails";
import Treemap from "./Treemap";
import RPC from "./rpc";
import { useSavedState } from './state';

type DiskTreeParams = {
	treePath: string;
	filter: Filter<Usage>;
	userMap: Map<number, string>;
	groupMap: Map<number, string>;
	setTreePath: (v: string) => void;
}

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
	day = 24 * 60 * 60 * 1000,
	now = +Date.now(),
	colourFromAge = (lm: number) => {
		const diff = now - lm;

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
	Breadcrumb = ({ path, part, setPath }: { path: string; part: string; setPath: (path: string) => void }) => <li>
		<button title={`Jump To: ${part}`} onClick={() => setPath(path)}>{part}</button>
	</li>,
	makeBreadcrumbs = (path: string, setPath: (path: string) => void) => {
		let last = 0;

		const breadcrumbs = [
			<Breadcrumb key={`breadcrumb_root`} path="/" part="Root" setPath={setPath} />
		];

		while (true) {
			const pos = path.indexOf("/", last + 1);

			if (pos === -1) {
				break;
			}

			breadcrumbs.push(<Breadcrumb key={`breadcrumb_${breadcrumbs.length}`} path={path.slice(0, pos)} part={path.slice(last + 1, pos)} setPath={setPath} />);

			last = pos;
		}

		breadcrumbs.push(<span key={`breadcrumb_${breadcrumbs.length}`} tabIndex={0} aria-current="location">{path.slice(last + 1) || "/"}</span>);

		return breadcrumbs;
	},
	determineTreeWidth = () => Math.max(window.innerWidth - 420, 400),
	makeFilter = (path: string, filter: Filter<Usage>, filetypes: string[], users: Map<number, string>, groups: Map<number, string>) => ({
		path,
		"users": (filter.UID as null | number[])?.map(uid => users.get(uid) ?? -1).join(",") ?? "",
		"groups": (filter.GID as null | number[])?.map(gid => groups.get(gid) ?? -1).join(",") ?? "",
		"types": filetypes.join(",")
	}),
	fileTypes = [
		"other", "temp", "vcf", "vcf.gz", "bcf", "sam", "bam",
		"cram", "fasta", "fastq", "fastq.gz", "ped/bed", "compressed", "text",
		"log", "dir"
	] as const,
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
	] as const,
	DiskTreeComponent = ({ treePath, filter, userMap, groupMap, setTreePath }: DiskTreeParams) => {
		const [treeMapData, setTreeMapData] = useState<Entry[] | null>(null),
			[breadcrumbs, setBreadcrumbs] = useState<JSX.Element[]>([]),
			[childDetails, setChildDetails] = useState<Child | null>(null),
			[dirDetails, setDirDetails] = useState<Child | null>(childDetails),
			[useMTime, setUseMTime] = useSavedState("useMTime", false),
			[useCount, setUseCount] = useSavedState("useCount", false),
			[treeWidth, setTreeWidth] = useState(determineTreeWidth()),
			[filterFileTypes, setFilterFileTypes] = useSavedState<string[]>("treeTypes", []),
			[sinceLastAccess, setSinceLastAccess] = useSavedState("sinceLastAccess", 0),
			[hasAuth, setHasAuth] = useState(true),
			filterStr = JSON.stringify(filter);

		useEffect(() => window.addEventListener("resize", () => setTreeWidth(determineTreeWidth())), []);

		useEffect(() => {
			RPC.getChildren(makeFilter(treePath, filter, filterFileTypes, userMap, groupMap))
				.then(children => {
					const entries: Entry[] = [],
						since = Date.now() - sinceLastAccess * 86_400_000;

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
		}, [treePath, useMTime, useCount, filterFileTypes, sinceLastAccess, filterStr]);

		return <>
			<details open className="boxed">
				<summary><h1>Disktree</h1></summary>
				<div id="disktree">
					<div className="treeFilter">
						<div className="title">Colour By</div>
						<label aria-label="Colour by Oldest Access Time" title="Oldest Access Time" htmlFor="aTime">Access Time</label><input type="radio" id="aTime" checked={!useMTime} onChange={() => setUseMTime(false)} />
						<label aria-label="Colour by Latest Modified Time" title="Latest Modified Time" htmlFor="mTime">Modified Time</label><input type="radio" id="mTime" checked={useMTime} onChange={() => setUseMTime(true)} />
						<div className="title">Area Represents</div>
						<label aria-label="Area represents File Size" htmlFor="useSize">File Size</label><input type="radio" id="useSize" checked={!useCount} onChange={() => setUseCount(false)} />
						<label aria-label="Area represents File Count" htmlFor="useCount">File Count</label><input type="radio" id="useCount" checked={useCount} onChange={() => setUseCount(true)} />
						<div className="title">Filter</div>
						<label htmlFor="filetypes">File Types</label><MultiSelect id="filetypes" list={fileTypes} onchange={setFilterFileTypes} />
						<label htmlFor="sinceAccess">Time Since Access</label>
						<select value={sinceLastAccess} id="sinceAccess" onChange={e => setSinceLastAccess(parseInt(e.target.value) ?? 0)}>
							{timesSinceAccess.map(([l, t]) => <option key={`tsa_${t}`} value={t}>{l}</option>)}
						</select>
					</div>
					<ul id="treeBreadcrumbs">{breadcrumbs}</ul>
					<div>
						<Treemap table={treeMapData} width={treeWidth} height={500} noAuth={!hasAuth} onmouseout={() => setChildDetails(dirDetails)} />
						<TreeDetails details={childDetails} style={{ width: treeWidth + "px" }} />
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
					</div>
				</div>
			</details>
		</>
	};

export default DiskTreeComponent;