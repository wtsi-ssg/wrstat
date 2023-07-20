import DiskTree from "./DiskTree";
import FilteredTable from "./FilteredTable";
import Filter from "./Filter"
import History from './History';
import SubDirs from './SubDirs';
import type { Usage } from "./rpc"
import { clearState, useSavedState } from "./state";
import Tabs from "./Tabs";
import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { asDaysAgo } from "./format";
import { fitlerTableRows } from "./Table";
import Scatter from "./Scatter";

const App = ({ groupUsage, userUsage, areas }: { groupUsage: Usage[], userUsage: Usage[], areas: Record<string, string[]> }) => {
	const [byUser, setBy] = useSavedState("byUser", false),
		[users, setUsers] = useState<number[]>([]),
		[groups, setGroups] = useState<number[]>([]),
		[owners, setOwners] = useSavedState<string[]>("owners", []),
		[axisMinSize, setAxisMinSize] = useSavedState("filterMinSize", -Infinity),
		[axisMaxSize, setAxisMaxSize] = useSavedState("filterMaxSize", Infinity),
		[axisMinDaysAgo, setAxisMinDaysAgo] = useSavedState("filterMinDaysAgo", -Infinity),
		[axisMaxDaysAgo, setAxisMaxDaysAgo] = useSavedState("filterMaxDaysAgo", Infinity),
		[savedMinSize, setSavedMinSize] = useSavedState("minSize", -Infinity),
		[savedMaxSize, setSavedMaxSize] = useSavedState("maxSize", Infinity),
		[savedMinDaysAgo, setSavedMinDaysAgo] = useSavedState("minDaysAgo", -Infinity),
		[savedMaxDaysAgo, setSavedMaxDaysAgo] = useSavedState("maxDaysAgo", Infinity),
		[filterMinDaysAgo, setFilterMinDaysAgo] = useState(savedMinDaysAgo),
		[filterMaxDaysAgo, setFilterMaxDaysAgo] = useState(savedMaxDaysAgo),
		[filterMinSize, setFilterMinSize] = useState(savedMinSize),
		[filterMaxSize, setFilterMaxSize] = useState(savedMaxSize),
		[selectedDir, setSelectedDir] = useSavedState("selectedDir", ""),
		[selectedID, setSelectedID] = useSavedState("selectedID", -1),
		usage = byUser ? userUsage : groupUsage,
		selectedRow = usage.filter(u => (byUser ? u.UID : u.GID) === selectedID && u.BaseDir === selectedDir)[0],
		[treePath, setTreePath] = useSavedState("treePath", "/"),
		groupNameToIDMap = new Map<string, number>(groupUsage.map(({ GID, Name }) => [Name || (GID + ""), GID])),
		userNameToIDMap = new Map<string, number>(userUsage.map(({ UID, Name }) => [Name || (UID + ""), UID])),
		userMap = new Map(Array.from(userNameToIDMap).map(([username, uid]) => [uid, username])),
		groupMap = new Map(Array.from(groupNameToIDMap).map(([groupname, gid]) => [gid, groupname])),
		baseFilter = byUser ? {
			UID: users,
			GIDs: (gids: number[]) => groups.length ? gids.some(gid => groups.includes(gid)) : true,
		} : {
			GID: groups,
			UIDs: (uids: number[]) => users.length ? uids.some(uid => users.includes(uid)) : true,
			Owner: owners
		},
		filter = Object.assign({
			UsageSize: { min: Math.max(filterMinSize, axisMinSize), max: Math.min(filterMaxSize, axisMaxSize) },
			Mtime: (mtime: string) => {
				const daysAgo = asDaysAgo(mtime);

				return daysAgo >= Math.max(filterMinDaysAgo, axisMinDaysAgo) && daysAgo <= Math.min(filterMaxDaysAgo, axisMaxDaysAgo);
			}
		}, baseFilter),
		primaryFilter = useRef<HTMLDivElement>(null),
		calculateScatterWidth = (div: HTMLDivElement) => Math.min(Math.max(300, parseInt(getComputedStyle(div).gridTemplateColumns.split(" ")[1]) - 40), 1500),
		[scaleSize, setScaleSize] = useSavedState("scaleSize", false),
		[scaleDays, setScaleDays] = useSavedState("scaleDays", false),
		[scatterWidth, setScatterWidth] = useState(300),
		scatterFilter = Object.assign({
			UsageSize: { min: axisMinSize, max: axisMaxSize },
			Mtime: (mtime: string) => {
				const daysAgo = asDaysAgo(mtime);

				return daysAgo >= axisMinDaysAgo && daysAgo <= axisMaxDaysAgo;
			}
		}, baseFilter),
		preview = savedMinSize !== filterMinSize || savedMaxSize !== filterMaxSize || savedMinDaysAgo !== filterMinDaysAgo || savedMaxDaysAgo !== filterMaxDaysAgo;

	if (!preview && selectedDir !== "" && selectedID !== -1 && fitlerTableRows(usage.filter(u => (byUser ? u.UID : u.GID) === selectedID && u.BaseDir === selectedDir), filter).length === 0) {
		setSelectedDir("");
		setSelectedID(-1);
	}

	useEffect(() => {
		setFilterMinSize(savedMinSize);
		setFilterMaxSize(savedMaxSize);
		setFilterMinDaysAgo(savedMinDaysAgo);
		setFilterMaxDaysAgo(savedMaxDaysAgo);
	}, [savedMinDaysAgo, savedMaxDaysAgo, savedMinSize, savedMaxSize]);

	useEffect(() => {
		window.addEventListener("resize", () => {
			if (primaryFilter.current) {
				setScatterWidth(calculateScatterWidth(primaryFilter.current));
			}
		});
	}, []);

	useLayoutEffect(() => {
		if (primaryFilter.current) {
			setScatterWidth(calculateScatterWidth(primaryFilter.current));
		}
	});

	return <>
		<Tabs id="mainTabs" tabs={[
			{
				title: "By Group",
				onClick: () => {
					clearState();
					setBy(false);
				},
				selected: !byUser
			},
			{
				title: "By User",
				onClick: () => {
					clearState();
					setBy(true);
				},
				selected: byUser
			},
		]} />
		<details open className="boxed">
			<summary><h1>Filter</h1></summary>
			<div className="primaryFilter" ref={primaryFilter}>
				<Filter {...({
					userUsage,
					groupUsage,
					areas,
					byUser,
					users, setUsers,
					groups, setGroups,
					owners, setOwners,
					axisMinSize, setAxisMinSize,
					axisMaxSize, setAxisMaxSize,
					axisMinDaysAgo, setAxisMinDaysAgo,
					axisMaxDaysAgo, setAxisMaxDaysAgo,
					scaleSize, setScaleSize,
					scaleDays, setScaleDays,
					groupNameToIDMap,
					userNameToIDMap
				})} />
				<Scatter
					width={scatterWidth}
					height={400}
					data={fitlerTableRows(byUser ? userUsage : groupUsage, scatterFilter)}
					logX={scaleDays}
					logY={scaleSize}
					minX={savedMinDaysAgo}
					maxX={savedMaxDaysAgo}
					minY={savedMinSize}
					maxY={savedMaxSize}
					isSelected={(u: any) => (byUser ? u.UID : u.GID) === selectedID && u.BaseDir === selectedDir}
					setLimits={(minS, maxS, minD, maxD) => {
						setSavedMinSize(minS);
						setSavedMaxSize(maxS);
						setSavedMinDaysAgo(minD);
						setSavedMaxDaysAgo(maxD);
						setFilterMinSize(minS);
						setFilterMaxSize(maxS);
						setFilterMinDaysAgo(minD);
						setFilterMaxDaysAgo(maxD);
					}} previewLimits={(minS, maxS, minD, maxD) => {
						setFilterMinSize(minS);
						setFilterMaxSize(maxS);
						setFilterMinDaysAgo(minD);
						setFilterMaxDaysAgo(maxD);
					}} />
			</div>
		</details >
		<FilteredTable
			{...{
				userMap,
				groupMap,
				usage,
				byUser,
				selectedID,
				setSelectedID,
				selectedDir,
				setSelectedDir,
				setTreePath,
				filter
			}} />
		<DiskTree
			{...{
				userMap,
				groupMap,
				treePath,
				setTreePath,
				filter
			}} />
		<SubDirs
			id={selectedID}
			path={selectedDir}
			isUser={byUser}
			setPath={setTreePath} />
		<History
			id={selectedID}
			path={selectedDir}
			isUser={byUser}
			name={selectedRow?.Name}
			owner={selectedRow?.Owner} />
	</>
};

export default App;