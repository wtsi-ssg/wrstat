import type {Usage} from "./rpc";
import {useState} from "react";
import FilteredTable from "./filteredTable";
import {asDaysAgo} from "./format";
import MultiSelect from "./multiselect";
import Scatter from "./scatter";
import {useSavedState, useEffectAfterInit} from './state';
import {fitlerTableRows} from "./table";

const stringSort = new Intl.Collator().compare;

export default ({groupUsage, userUsage, areas}: {groupUsage: Usage[], userUsage: Usage[], areas: Record<string, string[]>}) => {
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
	groupMap = new Map<string, number>(groupUsage.map(({GID, Name}) => [Name || (GID + ""), GID])),
	userMap = new Map<string, number>(userUsage.map(({UID, Name}) => [Name || (UID + ""), UID])),
	allGroups = groups.concat(boms.map(b => areas[b].map(a => groupMap.get(a) ?? -1)).flat()),
	ofilter = {
		UID: byUser ? users : undefined,
		GID: byUser ? undefined : allGroups,
		UIDs: byUser ? undefined : (uids: number[]) => users.length ? uids.some(uid => users.includes(uid)) : true,
		GIDs: byUser ? (gids: number[]) => allGroups.length ? gids.some(gid => allGroups.includes(gid)) : true : undefined,
		Owner: byUser ? [] : owners
	},
	filter = Object.assign({
		UsageSize: {min: minSize, max: maxSize},
		Mtime: (mtime: string) => {
			const daysAgo = asDaysAgo(mtime);

			return daysAgo >= minDaysAgo && daysAgo <= maxDaysAgo;
		}
	}, ofilter);

	useEffectAfterInit(() => {
		setMinSize(savedMinSize);
		setMaxSize(savedMaxSize);
		setMinDaysAgo(savedMinDaysAgo);
		setMaxDaysAgo(savedMaxDaysAgo);
	}, [savedMinDaysAgo, savedMaxDaysAgo, savedMinSize, savedMaxSize]);

	useEffectAfterInit(() => {
		setSavedMinSize(-Infinity);
		setSavedMaxSize(Infinity);
		setSavedMinDaysAgo(-Infinity);
		setSavedMaxDaysAgo(Infinity);
		setMinSize(-Infinity);
		setMaxSize(Infinity);
		setMinDaysAgo(-Infinity);
		setMaxDaysAgo(Infinity);
	}, [byUser]);

	return <>
		<details open>
			<summary>Filter</summary>
			<div className="treeFilter">
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
				<label htmlFor="scaleSize">Log Size</label>
				<input type="checkbox" id="scaleSize" checked={scaleSize} onChange={e => setScaleSize(e.target.checked)} />
				<label htmlFor="scaleDays">Log Days</label>
				<input type="checkbox" id="scaleDays" checked={scaleDays} onChange={e => setScaleDays(e.target.checked)} />
			</div>
		</details>
		<FilteredTable users={userMap} groups={groupMap} usage={byUser ? userUsage : groupUsage} byUser={byUser} {...filter} />
	</>
}
