import type { Usage } from "./rpc";
import { useEffect, useLayoutEffect, useRef, useState } from "react";
import FilteredTable from "./filteredTable";
import { asDaysAgo, formatBytes, formatNumber } from "./format";
import MultiSelect, { type Listener } from "./multiselect";
import Scatter from "./scatter";
import { clearState, useSavedState } from './state';
import { fitlerTableRows } from "./table";
import Minmax from "./minmax";
import Tabs from "./tabs";

const stringSort = new Intl.Collator().compare,
	calculateScatterWidth = (div: HTMLDivElement) => Math.min(Math.max(300, parseInt(getComputedStyle(div).gridTemplateColumns.split(" ")[1]) - 40), 1500);

const FilterComponent = ({ groupUsage, userUsage, areas }: { groupUsage: Usage[], userUsage: Usage[], areas: Record<string, string[]> }) => {
	const [byUser, setBy] = useSavedState("byUser", false),
		[users, setUsers] = useState<number[]>([]),
		[groups, setGroups] = useState<number[]>([]),
		[owners, setOwners] = useSavedState<string[]>("owners", []),
		[scaleSize, setScaleSize] = useSavedState("scaleSize", false),
		[scaleDays, setScaleDays] = useSavedState("scaleDays", false),
		[savedMinSize, setSavedMinSize] = useSavedState("minSize", -Infinity),
		[savedMaxSize, setSavedMaxSize] = useSavedState("maxSize", Infinity),
		[savedMinDaysAgo, setSavedMinDaysAgo] = useSavedState("minDaysAgo", -Infinity),
		[savedMaxDaysAgo, setSavedMaxDaysAgo] = useSavedState("maxDaysAgo", Infinity),
		[minSize, setMinSize] = useState(savedMinSize),
		[maxSize, setMaxSize] = useState(savedMaxSize),
		[minDaysAgo, setMinDaysAgo] = useState(savedMinDaysAgo),
		[maxDaysAgo, setMaxDaysAgo] = useState(savedMaxDaysAgo),
		[filterMinSize, setFilterMinSize] = useSavedState("filterMinSize", -Infinity),
		[filterMaxSize, setFilterMaxSize] = useSavedState("filterMaxSize", Infinity),
		[filterMinDaysAgo, setFilterMinDaysAgo] = useSavedState("filterMinDaysAgo", -Infinity),
		[filterMaxDaysAgo, setFilterMaxDaysAgo] = useSavedState("filterMaxDaysAgo", Infinity),
		[scatterWidth, setScatterWidth] = useState(300),
		[selectedDir, setSelectedDir] = useSavedState("selectedDir", ""),
		[selectedID, setSelectedID] = useSavedState("selectedID", -1),
		usage = byUser ? userUsage : groupUsage,
		primaryFilter = useRef<HTMLDivElement>(null),
		groupNameToIDMap = new Map<string, number>(groupUsage.map(({ GID, Name }) => [Name || (GID + ""), GID])),
		groupIDToNameMap = new Map<number, string>(groupUsage.map(({ GID, Name }) => [GID, Name || (GID + "")])),
		userNameToIDMap = new Map<string, number>(userUsage.map(({ UID, Name }) => [Name || (UID + ""), UID])),
		basefilter = {
			UID: byUser ? users : undefined,
			GID: byUser ? undefined : groups,
			UIDs: byUser ? undefined : (uids: number[]) => users.length ? uids.some(uid => users.includes(uid)) : true,
			GIDs: byUser ? (gids: number[]) => groups.length ? gids.some(gid => groups.includes(gid)) : true : undefined,
			Owner: byUser ? [] : owners
		},
		scatterFilter = Object.assign({
			UsageSize: { min: filterMinSize, max: filterMaxSize },
			Mtime: (mtime: string) => {
				const daysAgo = asDaysAgo(mtime);

				return daysAgo >= filterMinDaysAgo && daysAgo <= filterMaxDaysAgo;
			}
		}, basefilter),
		tableFilter = Object.assign({
			UsageSize: { min: Math.max(minSize, filterMinSize), max: Math.min(maxSize, filterMaxSize) },
			Mtime: (mtime: string) => {
				const daysAgo = asDaysAgo(mtime);

				return daysAgo >= Math.max(minDaysAgo, filterMinDaysAgo) && daysAgo <= Math.min(maxDaysAgo, filterMaxDaysAgo);
			}
		}, basefilter),
		groupSet = new Set(groups),
		selectedBOMs = Object.entries(areas).map(([bom, groups]) => groups.every(g => groupNameToIDMap.get(g) === undefined || groupSet.has(groupNameToIDMap.get(g)!)) ? bom : "").filter(b => b).sort(stringSort),
		preview = savedMinSize !== minSize || savedMaxSize !== maxSize || savedMinDaysAgo !== minDaysAgo || savedMaxDaysAgo !== maxDaysAgo;

	if (!preview && selectedDir !== "" && selectedID !== -1 && fitlerTableRows(usage.filter(u => (byUser ? u.UID : u.GID) === selectedID && u.BaseDir === selectedDir), tableFilter).length === 0) {
		setSelectedDir("");
		setSelectedID(-1);
	}

	useEffect(() => {
		setMinSize(savedMinSize);
		setMaxSize(savedMaxSize);
		setMinDaysAgo(savedMinDaysAgo);
		setMaxDaysAgo(savedMaxDaysAgo);
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

	let groupPipe: Listener | null = null;

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
			<summary>Filter</summary>
			<div className="primaryFilter" ref={primaryFilter}>
				<div className="treeFilter">
					<label htmlFor="owners">Owners</label>
					<MultiSelect id="owners" list={Array.from(new Set(groupUsage.map(e => e.Owner).filter(o => o)).values()).sort(stringSort)} onchange={setOwners} disabled={byUser} />
					<label htmlFor="bom">Group Areas</label>
					<MultiSelect id="bom" list={Object.keys(areas).sort(stringSort)} selectedList={selectedBOMs} onchange={(boms, deleted) => {
						if (deleted) {
							groupPipe?.(areas[deleted], true);

							return false;
						}

						const existingGroups = new Set(groups.map(gid => groupIDToNameMap.get(gid) ?? ""));

						for (const bom of boms) {
							for (const g of areas[bom] ?? []) {
								if (groupNameToIDMap.has(g)) {
									existingGroups.add(g);
								}
							}
						}

						groupPipe?.(Array.from(existingGroups), false);

						return false;
					}} />
					<label htmlFor="unix">Unix Group</label>
					<MultiSelect id="unix" list={Array.from(new Set(groupUsage.map(e => e.Name)).values()).sort(stringSort)} listener={(cb: Listener) => groupPipe = cb} onchange={groups => setGroups(groups.map(groupname => groupNameToIDMap.get(groupname) ?? -1))} />
					<label htmlFor="username">Username</label>
					<MultiSelect id="username" list={Array.from(new Set(userUsage.map(e => e.Name)).values()).sort(stringSort)} onchange={users => setUsers(users.map(username => userNameToIDMap.get(username) ?? -1))} />
					<label>Size </label>
					<Minmax max={usage.reduce((max, curr) => Math.max(max, curr.UsageSize), 0)} width={300} minValue={filterMinSize} maxValue={filterMaxSize} onchange={(min: number, max: number) => {
						setFilterMinSize(min);
						setFilterMaxSize(max);
					}} formatter={formatBytes} />
					<label>Last Modified</label>
					<Minmax max={usage.reduce((curr, next) => Math.max(curr, asDaysAgo(next.Mtime)), 0)} minValue={filterMinDaysAgo} maxValue={filterMaxDaysAgo} width={300} onchange={(min: number, max: number) => {
						setFilterMinDaysAgo(min);
						setFilterMaxDaysAgo(max);
					}} formatter={formatNumber} />
					<label htmlFor="scaleSize">Log Size Axis</label>
					<input type="checkbox" id="scaleSize" checked={scaleSize} onChange={e => setScaleSize(e.target.checked)} />
					<label htmlFor="scaleDays">Log Days Axis</label>
					<input type="checkbox" id="scaleDays" checked={scaleDays} onChange={e => setScaleDays(e.target.checked)} />
					<button onClick={clearState}>Reset Filter</button>
				</div>
				<Scatter width={scatterWidth} height={400} data={fitlerTableRows(byUser ? userUsage : groupUsage, scatterFilter)} logX={scaleDays} logY={scaleSize} minX={savedMinDaysAgo} maxX={savedMaxDaysAgo} minY={savedMinSize} maxY={savedMaxSize} isSelected={(u: any) => (byUser ? u.UID : u.GID) === selectedID && u.BaseDir === selectedDir} setLimits={(minS, maxS, minD, maxD) => {
					setSavedMinSize(minS);
					setSavedMaxSize(maxS);
					setSavedMinDaysAgo(minD);
					setSavedMaxDaysAgo(maxD);
					setMinSize(minS);
					setMaxSize(maxS);
					setMinDaysAgo(minD);
					setMaxDaysAgo(maxD);
				}} previewLimits={(minS, maxS, minD, maxD) => {
					setMinSize(minS);
					setMaxSize(maxS);
					setMinDaysAgo(minD);
					setMaxDaysAgo(maxD);
				}} />
			</div>
		</details >
		<FilteredTable users={userNameToIDMap} groups={groupNameToIDMap} usage={usage} {...{ byUser, selectedID, setSelectedID, selectedDir, setSelectedDir }} filter={tableFilter} />
	</>
};

export default FilterComponent;