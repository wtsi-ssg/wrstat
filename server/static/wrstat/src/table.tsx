import {useState, type CSSProperties} from "react";
import Pagination from "./pagination";

type Column<T> = { [K in Extract<keyof T, string>]-?: {
	title: string;
	key: K;
	extra?: (data: T[K], row: T) => Record<string, any>;
	formatter?: (data: T[K], row: T) => JSX.Element | string;
	sortFn?: (a: T, b: T) => number;
	reverseFn?: (a: T, b: T) => number;
	startReverse?: boolean;
}}[Extract<keyof T, string>];

type Filter<T> = {
	[K in keyof T]?: T[K][];
}

type Params<T> = {
	table: T[];
	cols: Column<T>[];
	perPage?: number;
	filter?: Filter<T>;
	rowExtra?: (row: T) => Record<string, any>;
	onRowClick: (row: T) => void;
	className?: string;
	id?: string;
	style?: CSSProperties;
}

const noopFormatter = (a: {toString(): string}) => a.toString(),
noop = () => {},
noopSort = () => 0,
reverseSort = <T extends Record<string, any>,>(fn: (a: T, b: T) => number) => (a: T, b: T) => fn(b, a),
getSorter = <T extends Record<string, any>>(col: Column<T>, reverse = false) => {
	if (!reverse) {
		return col["sortFn"] ?? noopSort;
	}

	return col["reverseFn"] ?? reverseSort(col["sortFn"] ?? noopSort);
};

export const fitlerTableRows = <T extends Record<string, any>>(table: T[], filter: Filter<T>) => {
	const toRet: T[] = [],
	filterKeys = Object.keys(filter) as (keyof Filter<T>)[];

	for (const row of table) {
		let add = true;

		for (const f of filterKeys) {
			const toFilter = filter[f];
			if (toFilter) {
				if (toFilter instanceof Array) {
					if (toFilter.length && toFilter.includes(row[f])) {
						add = false;

						break;
					}
				}
			}	
		}

		if (add) {
			toRet.push(row);
		}
	}

	return toRet;
};

export default <T extends Record<string, any>>({table, cols, onRowClick, perPage = Infinity, filter = {}, rowExtra,...additional}: Params<T>) => {
	const [page, setPage] = useState(0),
	[sortBy, setSortBy] = useState(-1),
	[sortReverse, setSortReverse] = useState(false),
	rows = fitlerTableRows(table, filter);

	if (sortBy >= 0) {
		rows.sort(getSorter(cols[sortBy], sortReverse))
	}

	return <>
		<Pagination currentPage={page} onClick={e => setPage(parseInt((e.target as HTMLElement).dataset["page"] || "0"))} totalPages={Math.ceil(rows.length / perPage)} />
		<table {...additional}>
			<thead>
				<tr>
					{cols.map((c, n) => <th className={c.sortFn ? "sortable" + (sortBy === n ? " sort" + (sortReverse ? " reverse" : "") : "") : ""} onClick={c.sortFn ? () => {
						if (sortBy === n) {
							setSortReverse(!sortReverse);
						} else {
							setSortBy(n);
							setSortReverse(cols[n].startReverse ?? false);
						}
					} : noop}>{c.title}</th>)}
				</tr>
			</thead>
			<tbody>
				{rows.slice(page * perPage, (page + 1) * perPage).map(row => <tr onClick={() => onRowClick(row)} {...(rowExtra?.(row) ?? {})}>
					{cols.map(col => <td {...(col.extra?.(row[col.key], row) ?? {})}>{(col.formatter ?? noopFormatter)(row[col.key], row)}</td>)}
				</tr>)}
			</tbody>
		</table>
	</>
}