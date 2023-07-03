import type {Child, History} from './rpc';
import type {Entry} from './treemap';
import {useEffect, useState, type ReactNode} from "react"
import HistoryGraph from './history';
import SubDirs from './subdirs';
import Treemap from "./treemap";
import TreeDetails from "./treedetails";
import rpc from "./rpc";

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
};

export default ({id, path, isUser, history}: {id: number, path: string; isUser: boolean; history: History[]}) => {
	const [treePath, setTreePath] = useState(path || "/"),
	[treeMapData, setTreeMapData] = useState<Entry[] | null>(null),
	[breadcrumbs, setBreadcrumbs] = useState<ReactNode[]>([]),
	[childDetails, setChildDetails] = useState<Child | null>(null),
	[dirDetails, setDirDetails] = useState<Child | null>(childDetails),
	[useMTime, setUseMTime] = useState(false),
	[useCount, setUseCount] = useState(false),
	[treeWidth, setTreeWidth] = useState(determineTreeWidth());

	useEffect(() => window.addEventListener("resize", () => setTreeWidth(determineTreeWidth())));

	useEffect(() => setTreePath(path || "/"), [path]);

	useEffect(() => {
		rpc.getChildren({"path": treePath})
		.then(children => {
			if (children.users.length === 0) {
				setTreeMapData(null);
				setChildDetails(null);
				setDirDetails(null);

				return;
			}

			const entries: Entry[] = [];

			for (const child of children.children ?? []) {
				entries.push({
					name: child.name,
					value: useCount ? child.count : child.size,
					backgroundColour: colourFromAge(+(new Date(useMTime ? child.timestamp : child.atime))),
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
	}, [treePath, useMTime, useCount]);

	return <>
		<ul id="treeBreadcrumbs">{breadcrumbs}</ul>
		<Treemap table={treeMapData} width={treeWidth} height={500} onmouseout={() => setChildDetails(dirDetails)} />
		<br />
		<label htmlFor="timeToggle">Use MTime: </label><input type="checkbox" id="timeToggle" checked={useMTime} onChange={e => setUseMTime(e.target.checked)} />
		<br />
		<label htmlFor="countToggle">Use Count: </label><input type="checkbox" id="countToggle" checked={useCount} onChange={e => setUseCount(e.target.checked)} />
		<TreeDetails details={childDetails} />
		<SubDirs id={id} path={path} isUser={isUser} setPath={setTreePath} />
		<HistoryGraph history={history} width={960} height={500} />
	</>
}