import type { Usage } from "./rpc";
import { useEffect, useRef, useState } from "react";
import FilteredTable from "./filteredTable";
import { asDaysAgo, formatBytes, formatNumber } from "./format";
import MultiSelect from "./multiselect";
import Scatter from "./scatter";
import { useSavedState } from './state';
import { fitlerTableRows } from "./table";
import Minmax from "./minmax";

const stringSort = new Intl.Collator().compare;

export default ({ groupUsage, userUsage, areas }: { groupUsage: Usage[], userUsage: Usage[], areas: Record<string, string[]> }) => {
	const [byUser, setBy] = useSavedState("byUser", false),
		[users, setUsers] = useState<number[]>([]),
		[groups, setGroups] = useState<number[]>([]),
		[boms, setBOMs] = useSavedState<string[]>("boms", []),
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
		[sliderWidth, setSliderWidth] = useState(300),
		treeFilter = useRef<HTMLDivElement>(null),
		groupMap = new Map<string, number>(groupUsage.map(({ GID, Name }) => [Name || (GID + ""), GID])),
		userMap = new Map<string, number>(userUsage.map(({ UID, Name }) => [Name || (UID + ""), UID])),
		allGroups = groups.concat(boms.map(b => areas[b].map(a => groupMap.get(a) ?? -1)).flat()).filter(gid => gid !== -1),
		basefilter = {
			UID: byUser ? users : undefined,
			GID: byUser ? undefined : allGroups,
			UIDs: byUser ? undefined : (uids: number[]) => users.length ? uids.some(uid => users.includes(uid)) : true,
			GIDs: byUser ? (gids: number[]) => allGroups.length ? gids.some(gid => allGroups.includes(gid)) : true : undefined
		},
		ofilter = Object.assign({
			Owner: byUser ? [] : owners,
			UsageSize: { min: filterMinSize, max: filterMaxSize },
			Mtime: (mtime: string) => {
				const daysAgo = asDaysAgo(mtime);

				return daysAgo >= filterMinDaysAgo && daysAgo <= filterMaxDaysAgo;
			}
		}, basefilter),
		filter = Object.assign({
			UsageSize: { min: Math.max(minSize, filterMinSize), max: Math.min(maxSize, filterMaxSize) },
			Mtime: (mtime: string) => {
				const daysAgo = asDaysAgo(mtime);

				return daysAgo >= Math.max(minDaysAgo, filterMinDaysAgo) && daysAgo <= Math.min(maxDaysAgo, filterMaxDaysAgo);
			}
		}, basefilter);

	useEffect(() => {
		setMinSize(savedMinSize);
		setMaxSize(savedMaxSize);
		setMinDaysAgo(savedMinDaysAgo);
		setMaxDaysAgo(savedMaxDaysAgo);
	}, [savedMinDaysAgo, savedMaxDaysAgo, savedMinSize, savedMaxSize]);

	useEffect(() => {
		setSavedMinSize(-Infinity);
		setSavedMaxSize(Infinity);
		setSavedMinDaysAgo(-Infinity);
		setSavedMaxDaysAgo(Infinity);
		setMinSize(-Infinity);
		setMaxSize(Infinity);
		setMinDaysAgo(-Infinity);
		setMaxDaysAgo(Infinity);
		setFilterMinSize(-Infinity);
		setFilterMaxSize(Infinity);
		setFilterMinDaysAgo(-Infinity);
		setFilterMaxDaysAgo(Infinity);
	}, [byUser]);

	useEffect(() => {
		window.addEventListener("resize", () => {
			if (treeFilter.current) {
				setSliderWidth(getComputedStyle(treeFilter.current).gridTemplateColumns.split(" ").slice(1, -1).map(e => parseInt(e)).reduce((a, b) => a + b, 0) - 10);
			}
		});

		if (treeFilter.current) {
			setSliderWidth(getComputedStyle(treeFilter.current).gridTemplateColumns.split(" ").slice(1, -1).map(e => parseInt(e)).reduce((a, b) => a + b, 0) - 10);
		}
	}, []);

	return <>
		<details open>
			<summary>Filter</summary>
			<div className="treeFilter" ref={treeFilter}>
				<label htmlFor="byGroup">By Group</label>
				<input type="radio" name="by" id="byGroup" checked={!byUser} onChange={e => setBy(!e.target.checked)} />
				<label htmlFor="byUser">By User</label>
				<input type="radio" name="by" id="byUser" checked={byUser} onChange={e => setBy(e.target.checked)} />
				<Scatter width={900} height={400} data={fitlerTableRows(byUser ? userUsage : groupUsage, ofilter)} logX={scaleDays} logY={scaleSize} minX={savedMinDaysAgo} maxX={savedMaxDaysAgo} minY={savedMinSize} maxY={savedMaxSize} setLimits={(minS, maxS, minD, maxD) => {
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
				<label htmlFor="username">Username</label>
				<MultiSelect id="username" list={Array.from(new Set(userUsage.map(e => e.Name)).values()).sort(stringSort)} onchange={users => setUsers(users.map(username => userMap.get(username) ?? -1))} />
				<label htmlFor="unix">Unix Group</label>
				<MultiSelect id="unix" list={Array.from(new Set(groupUsage.map(e => e.Name)).values()).sort(stringSort)} onchange={groups => setGroups(groups.map(groupname => groupMap.get(groupname) ?? -1))} />
				<label htmlFor="owners">Owners</label>
				<MultiSelect id="owners" list={Array.from(new Set(groupUsage.map(e => e.Owner).filter(o => o)).values()).sort(stringSort)} onchange={setOwners} />
				<label htmlFor="bom">Group Areas</label>
				<MultiSelect id="bom" list={Object.keys(areas).sort(stringSort)} onchange={setBOMs} />
				<label>Size </label>
				<Minmax max={userUsage.concat(groupUsage).map(u => u.UsageSize).reduce((max, curr) => Math.max(max, curr), 0)} width={sliderWidth} minValue={filterMinSize} maxValue={filterMaxSize} onchange={(min: number, max: number) => {
					setFilterMinSize(min);
					setFilterMaxSize(max);
				}} formatter={formatBytes} />
				<label>Last Modified</label>
				<Minmax max={userUsage.concat(groupUsage).map(e => asDaysAgo(e.Mtime)).reduce((curr, next) => Math.max(curr, next), 0)} minValue={filterMinDaysAgo} maxValue={filterMaxDaysAgo} width={sliderWidth} onchange={(min: number, max: number) => {
					setFilterMinDaysAgo(min);
					setFilterMaxDaysAgo(max);
				}} formatter={formatNumber} />
				<label htmlFor="scaleSize">Log Size Axis</label>
				<input type="checkbox" id="scaleSize" checked={scaleSize} onChange={e => setScaleSize(e.target.checked)} />
				<label htmlFor="scaleDays">Log Days Axis</label>
				<input type="checkbox" id="scaleDays" checked={scaleDays} onChange={e => setScaleDays(e.target.checked)} />
			</div>
		</details>
		<FilteredTable users={userMap} groups={groupMap} usage={byUser ? userUsage : groupUsage} byUser={byUser} {...filter} />
	</>
}
