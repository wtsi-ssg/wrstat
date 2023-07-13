import type { Usage } from './rpc';
import type { ChangeEvent } from "react";
import { useEffect } from "react";
import { downloadGroups, downloadUsers } from './download';
import { asDaysAgoStr, formatBytes, formatNumber } from './format';
import PathDetails from './pathDetails';
import { useSavedState } from './state';
import Table, { type Filter, fitlerTableRows } from './table';

const stringSort = new Intl.Collator().compare,
	sorters = [
		(a: Usage, b: Usage) => a.Owner === "" ? 1 : b.Owner === "" ? -1 : stringSort(a.Owner, b.Owner) || stringSort(a.BaseDir, b.BaseDir),
		(a: Usage, b: Usage) => stringSort(a.Name, b.Name) || stringSort(a.BaseDir, b.BaseDir),
		(a: Usage, b: Usage) => stringSort(a.BaseDir, b.BaseDir) || stringSort(a.Name, b.Name),
		(a: Usage, b: Usage) => a.UsageSize - b.UsageSize || stringSort(a.BaseDir, b.BaseDir),
		(a: Usage, b: Usage) => a.QuotaSize - b.QuotaSize || stringSort(a.BaseDir, b.BaseDir),
		(a: Usage, b: Usage) => (b.QuotaSize * a.UsageSize - a.QuotaSize * b.UsageSize) || stringSort(a.BaseDir, b.BaseDir),
		(a: Usage, b: Usage) => a.UsageInodes - b.UsageInodes || stringSort(a.BaseDir, b.BaseDir),
		(a: Usage, b: Usage) => a.QuotaInodes - b.QuotaInodes || stringSort(a.BaseDir, b.BaseDir),
		(a: Usage, b: Usage) => (b.QuotaInodes * a.UsageInodes - a.QuotaInodes * b.UsageInodes) || stringSort(a.BaseDir, b.BaseDir),
		(a: Usage, b: Usage) => new Date(b.Mtime).valueOf() - new Date(a.Mtime).valueOf() || stringSort(a.BaseDir, b.BaseDir),
		(a: Usage, b: Usage) => stringSort(a.status ?? "", b.status ?? "") || stringSort(a.BaseDir, b.BaseDir),
	] as const,
	reverseSorters = [
		(a: Usage, b: Usage) => a.Owner === "" ? 1 : b.Owner === "" ? -1 : sorters[1](b, a),
		null,
		null,
		null,
		null,
		(a: Usage, b: Usage) => !a.QuotaSize ? 1 : !b.QuotaSize ? -1 : sorters[5](b, a),
		null,
		null,
		(a: Usage, b: Usage) => !a.QuotaInodes ? 1 : !b.QuotaInodes ? -1 : sorters[8](b, a),
		null,
		null,
	] as const;

const FilteredTableComponent = ({ usage, byUser, groups, users, ...filter }: Filter<Usage> & { byUser: boolean; usage: Usage[], users: Map<string, number>, groups: Map<string, number> }) => {
	const [selectedDir, setSelectedDir] = useSavedState("selectedDir", ""),
		[selectedID, setSelectedID] = useSavedState("selectedID", -1),
		[perPage, setPerPage] = useSavedState("perPage", 10),
		userMap = new Map(Array.from(users).map(([username, uid]) => [uid, username])),
		groupMap = new Map(Array.from(groups).map(([groupname, gid]) => [gid, groupname])),
		selectedRow = usage.filter(u => (byUser ? u.UID : u.GID) === selectedID && u.BaseDir === selectedDir)[0];

	useEffect(() => {
		setSelectedDir("");
		setSelectedID(-1);
	}, [byUser]);

	return <>
		<details open className="boxed">
			<summary>Basedirs</summary>
			<span id="perPage">Show
				<select onChange={(e: ChangeEvent<HTMLSelectElement>) => { setPerPage(parseInt(e.target.value) ?? 10) }}>
					<option selected={perPage === 10}>10</option>
					<option selected={perPage === 25}>25</option>
					<option selected={perPage === 50}>50</option>
					<option selected={perPage === 100}>100</option>
				</select>
				Entries</span>
			<Table rowExtra={row => {
				if ((byUser ? row.UID : row.GID) === selectedID && row.BaseDir === selectedDir) {
					return { "class": "selected" };
				}

				return {};
			}} perPage={perPage} filter={filter} onRowClick={(data: Usage) => {
				const id = byUser ? data.UID : data.GID;

				if (selectedDir === data.BaseDir && selectedID === id) {
					setSelectedDir("");
					setSelectedID(-1);
				} else {
					setSelectedDir(data.BaseDir);
					setSelectedID(id);
				}
			}} cols={[
				{
					title: "Path",
					key: "BaseDir",
					extra: (path, row) => ({
						title: path +
							(row.Owner ? "\nPI: " + row.Owner : "") +
							"\n" + (byUser ? "User: " : "Group: ") + row.Name +
							"\n" + (byUser ? "Groups: " : "Users: ") + (byUser ? row.GIDs : row.UIDs).map(id => (byUser ? groupMap : userMap).get(id) ?? "").filter(n => n).join(", ")
					}),
					sortFn: sorters[2]
				},
				{
					title: "Space Used",
					key: "UsageSize",
					extra: used => ({ title: formatNumber(used) + " Bytes" }),
					sortFn: sorters[3],
					startReverse: true,
					formatter: formatBytes
				},
				{
					title: "Space Quota",
					key: "QuotaSize",
					extra: quota => ({ title: formatNumber(quota) + " Bytes" }),
					sortFn: sorters[4],
					startReverse: true,
					formatter: formatBytes
				},
				{
					title: "Space Usage (%)",
					key: "percentSize",
					sortFn: sorters[5],
					reverseFn: reverseSorters[5],
					startReverse: true,
					formatter: (p: number | undefined) => p ? formatNumber(p) : "0"
				},
				{
					title: "Num. Files",
					key: "UsageInodes",
					sortFn: sorters[6],
					startReverse: true,
					formatter: formatNumber
				},
				{
					title: "Max Files",
					key: "QuotaInodes",
					sortFn: sorters[7],
					startReverse: true,
					formatter: formatNumber
				},
				{
					title: "File Usage (%)",
					key: "percentInodes",
					sortFn: sorters[8],
					reverseFn: reverseSorters[8],
					startReverse: true,
					formatter: (p: number | undefined) => p ? formatNumber(p) : "0"
				},
				{
					title: "Last Modified (days)",
					key: "Mtime",
					extra: title => ({ title }),
					sortFn: sorters[9],
					formatter: asDaysAgoStr
				},
				{
					title: "Status",
					key: "status",
					extra: title => ({ title }),
					formatter: status => <svg xmlns="http://www.w3.org/2000/svg" style={{ width: "1em", height: "1em" }}><use href={status === "OK" ? "#ok" : "#notok"} /></svg>,
					sortFn: sorters[10]
				}
			]} table={usage} id="usageTable" className={"prettyTable " + (byUser ? "user" : "group")} />
			<button className="download" onClick={() => (byUser ? downloadUsers : downloadGroups)(usage)}>Download Unfiltered Table</button>
			<button className="download" onClick={() => (byUser ? downloadUsers : downloadGroups)(fitlerTableRows(usage, filter))}>Download Filtered Table</button>
		</details>
		<PathDetails id={selectedID} name={selectedRow?.Name} owner={selectedRow?.Owner} users={userMap} groups={groupMap} path={selectedDir} isUser={byUser} filter={filter} />
	</>
};

export default FilteredTableComponent;