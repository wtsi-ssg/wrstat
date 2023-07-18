import type { Usage } from "./rpc";
import { asDaysAgo, formatBytes, formatNumber } from "./format";
import Minmax from "./minmax";
import MultiSelect, { type Listener } from "./multiselect";
import { clearState } from './state';

type FilterParams = {
	groupUsage: Usage[];
	userUsage: Usage[];
	areas: Record<string, string[]>;
	byUser: boolean;
	users: number[];
	setUsers: (v: number[]) => void;
	groups: number[];
	setGroups: (v: number[]) => void;
	setOwners: (v: string[]) => void;
	axisMinSize: number;
	setAxisMinSize: (v: number) => void;
	axisMaxSize: number;
	setAxisMaxSize: (v: number) => void;
	axisMinDaysAgo: number;
	setAxisMinDaysAgo: (v: number) => void;
	axisMaxDaysAgo: number;
	setAxisMaxDaysAgo: (v: number) => void;
	scaleSize: boolean;
	setScaleSize: (v: boolean) => void;
	scaleDays: boolean;
	setScaleDays: (v: boolean) => void;
	groupNameToIDMap: Map<string, number>;
	userNameToIDMap: Map<string, number>;
}

const stringSort = new Intl.Collator().compare;

const FilterComponent = ({
	groupUsage,
	userUsage,
	areas,
	byUser,
	setUsers,
	groups, setGroups,
	setOwners,
	axisMinSize, setAxisMinSize,
	axisMaxSize, setAxisMaxSize,
	axisMinDaysAgo, setAxisMinDaysAgo,
	axisMaxDaysAgo, setAxisMaxDaysAgo,
	scaleSize, setScaleSize,
	scaleDays, setScaleDays,
	groupNameToIDMap,
	userNameToIDMap

}: FilterParams) => {
	const groupIDToNameMap = new Map<number, string>(groupUsage.map(({ GID, Name }) => [GID, Name || (GID + "")])),
		groupSet = new Set(groups),
		selectedBOMs = Object.entries(areas).map(([bom, groups]) => groups.every(g => groupNameToIDMap.get(g) === undefined || groupSet.has(groupNameToIDMap.get(g)!)) ? bom : "").filter(b => b).sort(stringSort),
		usage = byUser ? userUsage : groupUsage;

	let groupPipe: Listener | null = null;

	return <>
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
			<Minmax max={usage.reduce((max, curr) => Math.max(max, curr.UsageSize), 0)} width={300} minValue={axisMinSize} maxValue={axisMaxSize} onchange={(min: number, max: number) => {
				setAxisMinSize(min);
				setAxisMaxSize(max);
			}} formatter={formatBytes} />
			<label>Last Modified</label>
			<Minmax max={usage.reduce((curr, next) => Math.max(curr, asDaysAgo(next.Mtime)), 0)} minValue={axisMinDaysAgo} maxValue={axisMaxDaysAgo} width={300} onchange={(min: number, max: number) => {
				setAxisMinDaysAgo(min);
				setAxisMaxDaysAgo(max);
			}} formatter={formatNumber} />
			<label htmlFor="scaleSize">Log Size Axis</label>
			<input type="checkbox" id="scaleSize" checked={scaleSize} onChange={e => setScaleSize(e.target.checked)} />
			<label htmlFor="scaleDays">Log Days Axis</label>
			<input type="checkbox" id="scaleDays" checked={scaleDays} onChange={e => setScaleDays(e.target.checked)} />
			<button onClick={clearState}>Reset Filter</button>
		</div>

	</>
};

export default FilterComponent;