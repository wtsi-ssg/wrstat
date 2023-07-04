import type {History, TreeFilter, Usage} from './rpc';
import {useState, type ChangeEvent, useEffect} from "react";
import {downloadGroups, downloadUsers} from './download';
import {asDaysAgo, formatBytes, formatNumber} from './format';
import PathDetails from './pathDetails';
import fillQuotaSoon from './trend';
import RPC from './rpc';
import Table, {fitlerTableRows} from './table';

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
],
reverseSorters = [
	(a: Usage, b: Usage) => a.Owner === "" ? 1 : b.Owner === "" ? -1 : sorters[1](b, a),
	null,
	null,
	null,
	null,
	(a: Usage, b: Usage) => !a.QuotaSize ? 1 : !b.QuotaSize ? -1 : sorters[6](b, a),
	null,
	null,
	(a: Usage, b: Usage) => !a.QuotaInodes ? 1 : !b.QuotaInodes ? -1 : sorters[9](b, a),
	null,
	null,
] as const;

export default ({usage /*, history*/, ...filter}: TreeFilter & {usage: Usage[] /*, history: Map<string, History[]>*/}) => {
	const [selectedDir, setSelectedDir] = useState(""),
    [selectedID, setSelectedID] = useState(-1),
	[perPage, setPerPage] = useState(10),
	[history, setHistory] = useState<Map<string, History[]>>(new Map()),
	statusFormatter = (_: any, row: Usage) => {
		if (filter.byUser) {
			return "";
		}

		const gidBasename = row.GID + "|" + row.BaseDir,
		h = history.get(gidBasename);
		if (h) {
			switch (fillQuotaSoon(h)) {
			case false:
				return "OK";
			case true:
				return "Not OK";
			default:
				return "Unknown";
			}
		}

		RPC.getBasedirsHistory(row.GID, row.BaseDir)
		.then(h => {
			history.set(gidBasename, h);
			setHistory(new Map(history));
		})

		return "Unknown";
	},
	rowFilter = {
		Name: filter.name,
		Owner: filter.owner
	};

	useEffect(() => {
		setSelectedDir("");
		setSelectedID(-1);
	}, [filter.byUser]);

	return <>
		<details open>
			<summary>Basedirs</summary>
			<span id="perPage">Show
				<select onChange={(e: ChangeEvent<HTMLSelectElement>) => {setPerPage(parseInt(e.target.value) ?? 10)}}>
					<option>10</option>
					<option>25</option>
					<option>50</option>
					<option>100</option>
				</select>
			Entries</span>
			<Table rowExtra={row => {
				if ((filter.byUser ? row.UID : row.GID) === selectedID && row.BaseDir === selectedDir) {
					return {"class": "selected"};
				}

				return {};
			}} perPage={perPage} filter={rowFilter} onRowClick={(data: Usage) => {
				if (selectedDir === data.BaseDir && selectedID === (filter.byUser ? data.UID : data.GID)) {
						setSelectedDir("");
						setSelectedID(-1);
				} else {
						setSelectedDir(data.BaseDir);
						setSelectedID(filter.byUser ? data.UID : data.GID);
				}
			}} cols={[
				{
					title: "PI",
					key: "Owner",
					sortFn: sorters[0],
					reverseFn: reverseSorters[0],
				},
				{
					title: filter.byUser ? "User" : "Group",
					key: "Name",
					sortFn: sorters[1]
				},
				{
					title: "Path",
					key: "BaseDir",
					sortFn: sorters[2]
				},
				{
					title: "Space Used",
					key: "UsageSize",
					extra: used => ({title: formatNumber(used) + " Bytes"}),
					sortFn: sorters[3],
					startReverse: true,
					formatter: formatBytes
				},
				{
					title: "Space Quota",
					key: "QuotaSize",
					extra: quota => ({title: formatNumber(quota) + " Bytes"}),
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
					formatter: (p: number | undefined) => p ? formatNumber(p) : ""
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
					key: "percentSize",
					sortFn: sorters[8],
					reverseFn: reverseSorters[8],
					startReverse: true,
					formatter: (p: number | undefined) => p ? formatNumber(p) : ""
				},
				{
					title: "Last Modified (days)",
					key: "Mtime",
					extra: title => ({title}),
					sortFn: sorters[9],
					formatter: asDaysAgo
				},
				{
					title: "Status",
					key: "status",
					formatter: statusFormatter
				}
			]} table={usage} className={"prettyTable usageTable " + (filter.byUser ? "user" : "group")} />
			<button className="download" onClick={() => (filter.byUser ? downloadUsers : downloadGroups)(usage)}>Download Unfiltered Table</button>
			<button className="download" onClick={() => (filter.byUser ? downloadUsers : downloadGroups)(fitlerTableRows(usage, rowFilter))}>Download Filtered Table</button>
		</details>
		<PathDetails id={selectedID} path={selectedDir} isUser={filter.byUser} filter={filter} history={filter.byUser ? [] : history.get(selectedID + "|" + selectedDir) ?? []} />
	</>
}