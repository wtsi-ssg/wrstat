import type {History, TreeFilter, Usage} from './rpc';
import {useEffect, useState, type ChangeEvent} from "react";
import {downloadGroups, downloadUsers} from './download';
import {asDaysAgo, formatBytes, formatNumber} from './format';
import Pagination from './pagination';
import PathDetails from './pathDetails';
import fillQuotaSoon from './trend';
import RPC from './rpc';

const stringSort = new Intl.Collator().compare,
sorters = [
	(_a: Usage, _b: Usage) => 0,
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
reverse = (fn: (a: Usage, b: Usage) => number) => (a: Usage, b: Usage) => fn(b, a),
reverseSorters = [
	sorters[0],
	(a: Usage, b: Usage) => a.Owner === "" ? 1 : b.Owner === "" ? -1 : sorters[1](b, a),
	reverse(sorters[2]),
	reverse(sorters[3]),
	reverse(sorters[4]),
	reverse(sorters[5]),
	(a: Usage, b: Usage) => !a.QuotaSize ? 1 : !b.QuotaSize ? -1 : sorters[6](b, a),
	reverse(sorters[7]),
	reverse(sorters[8]),
	(a: Usage, b: Usage) => !a.QuotaInodes ? 1 : !b.QuotaInodes ? -1 : sorters[9](b, a),
	reverse(sorters[10]),
	reverse(sorters[11])
],
startReverse = [4, 5, 6, 7, 8, 9],
getTableRows = (usage: Usage[], filter: TreeFilter, page: number, perPage: number) => usage.filter(e => (!filter.name || e.Name === filter.name) &&
(!filter.owner || e.Owner === filter.owner)
).slice(page * perPage, (page + 1) * perPage);

export default ({usage /*, history*/, ...filter}: TreeFilter & {usage: Usage[] /*, history: Map<string, History[]>*/}) => {
	const [selectedDir, setSelectedDir] = useState(""),
	[selectedID, setSelectedID] = useState(-1),
	[sortBy, setSortBy] = useState(0),
	[sortReverse, setSortReverse] = useState(false),
	Header = ({sort, name}: {sort: number, name: string}) => <th className={sortBy === sort ? "sort" + (sortReverse ? " reverse" : "") : ""} onClick={() => {
		if (sortBy === sort) {
			setSortReverse(!sortReverse);
		} else {
			setSortBy(sort);
			setSortReverse(startReverse.includes(sort));
		}
	}}>{name}</th>,
	[page, setPage] = useState(0),
	[perPage, setPerPage] = useState(10),
	[history, setHistory] = useState<Map<string, History[]>>(new Map());

	useEffect(() => {
		setSortBy(0);
		setSortReverse(false);
		setPage(0);
	}, [filter.byUser, filter.name, filter.owner, perPage]);

	useEffect(() => {
		let changed = false,
		q = Promise.resolve();

		for (const row of getTableRows(usage, filter, page, perPage)) {
			const gidBasename = row.GID + "|" + row.BaseDir;
			if (history.has(gidBasename)) {
				continue;
			}

			changed = true;

			q = q.finally(() => RPC.getBasedirsHistory(row.GID, row.BaseDir)
			.then(h => {
				history.set(gidBasename, h);

				switch(fillQuotaSoon(h)) {
				case false:
					row.status = "OK";

					break;
				case true:
					row.status = "Not OK";

					break;
				default:
					row.status = "Unknown"
				}
			}));
		}

		q.finally(() => {
			if (changed) {
				setHistory(new Map(history));
			}
		});
	}, [filter.byUser, filter.name, filter.owner, page, perPage, sortBy, sortReverse]);

	usage.sort(sortReverse ? reverseSorters[sortBy] : sorters[sortBy]);

	return <>
		<Pagination currentPage={page} onClick={e => setPage(parseInt((e.target as HTMLElement).dataset["page"] || "0"))} totalPages={Math.ceil(getTableRows(usage, filter, 0, Infinity).length /perPage)} />
		<span id="perPage">Show
			<select onChange={(e: ChangeEvent<HTMLSelectElement>) => {setPerPage(parseInt(e.target.value) ?? 10)}}>
				<option>10</option>
				<option>25</option>
				<option>50</option>
				<option>100</option>
			</select>
		Entries</span>
		<table className={"prettyTable usageTable " + (filter.byUser ? "user" : "group")}>
			<thead>
				<tr>
					<Header sort={1} name="PI" />
					<Header sort={2} name={filter.byUser ? "User" : "Group"} />
					<Header sort={3} name="Path" />
					<Header sort={4} name="Space Used" />
					<Header sort={5} name="Space Quota" />
					<Header sort={6} name="Space Usage (%)" />
					<Header sort={7} name="Num. Files" />
					<Header sort={8} name="Max Files" />
					<Header sort={9} name="File Usage (%)" />
					<Header sort={10} name="Last Modified (days)" />
					<th>Status</th>
				</tr>
			</thead>
			<tbody>
				{getTableRows(usage, filter, page, perPage).map(data => {
					return (
						<tr className={data.BaseDir === selectedDir && (filter.byUser ? data.UID : data.GID) == selectedID ? "selected" : ""} onClick={() => {
							if (selectedDir === data.BaseDir && selectedID === (filter.byUser ? data.UID : data.GID)) {
								setSelectedDir("");
								setSelectedID(-1);
							} else {
								setSelectedDir(data.BaseDir);
								setSelectedID(filter.byUser ? data.UID : data.GID);
							}
						}}>
							<td>{data.Owner}</td>
							<td>{data.Name}</td>
							<td>{data.BaseDir}</td>
							<td title={formatNumber(data.UsageSize) + " Bytes"}>{formatBytes(data.UsageSize)}</td>
							<td title={formatNumber(data.QuotaSize) + " Bytes"}>{formatBytes(data.QuotaSize)}</td>
							<td>{data.QuotaSize === 0 ? "" : formatNumber(Math.round(10000 * data.UsageSize / data.QuotaSize) / 100)}</td>
							<td>{formatNumber(data.UsageInodes)}</td>
							<td>{formatNumber(data.QuotaInodes)}</td>
							<td>{data.QuotaInodes === 0 ? "" : formatNumber(Math.round(10000 * data.UsageInodes / data.QuotaInodes) / 100)}</td>
							<td>{asDaysAgo(data.Mtime)}</td>
							<td>{data.status ?? "Unknown"}</td>
						</tr>
					);
				})}
			</tbody>
		</table>
		<button className="download" onClick={() => (filter.byUser ? downloadUsers : downloadGroups)(usage)}>Download Unfiltered Table</button>
		<button className="download" onClick={() => (filter.byUser ? downloadUsers : downloadGroups)(getTableRows(usage, filter, 0, Infinity))}>Download Filtered Table</button>
		<br />
		<PathDetails id={selectedID} path={selectedDir} isUser={filter.byUser} history={filter.byUser ? [] : history.get(selectedID + "|" + selectedDir) ?? []} />
	</>
}