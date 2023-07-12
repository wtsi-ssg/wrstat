import { type CSSProperties } from "react";
import Pagination from "./pagination";
import { useSavedState } from "./state";

type Column<T> = {
	[K in Extract<keyof T, string>]-?: {
		title: string;
		key: K;
		extra?: (data: T[K], row: T) => Record<string, any>;
		formatter?: (data: T[K], row: T) => JSX.Element | string;
		sortFn?: (a: T, b: T) => number;
		reverseFn?: (a: T, b: T) => number;
		startReverse?: boolean;
	}
}[Extract<keyof T, string>];

export type Filter<T> = {
	[K in keyof T]?: ((col: T[K]) => boolean) | T[K][] | (T[K] extends number ? { min: number, max: number } : never);
}

type Params<T> = {
	table: T[];
	cols: Column<T>[];
	perPage?: number;
	filter?: Filter<T>;
	rowExtra?: (row: T) => Record<string, any>;
	onRowClick: (row: T) => void;
	className?: string;
	id: string;
	style?: CSSProperties;
}

const noopFormatter = (a: { toString(): string }) => a + "",
	noop = () => { },
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

	FilterLoop:
	for (const row of table) {
		for (const f of filterKeys) {
			const toFilter = filter[f];
			if (toFilter instanceof Array) {
				if (toFilter.length && !toFilter.includes(row[f])) {
					continue FilterLoop;
				}
			} else if (toFilter instanceof Function) {
				if (!toFilter(row[f])) {
					continue FilterLoop;
				}
			} else if (toFilter) {
				if (row[f] < toFilter.min || row[f] > toFilter.max) {
					continue FilterLoop;
				}
			}
		}

		toRet.push(row);
	}

	return toRet;
};

const TableComponent = <T extends Record<string, any>>({ table, cols, onRowClick, perPage = Infinity, filter = {}, id, rowExtra, ...additional }: Params<T>) => {
	const [page, setPage] = useSavedState(id + "Page", 0),
		[sortBy, setSortBy] = useSavedState(id + "Sort", -1),
		[sortReverse, setSortReverse] = useSavedState(id + "Reverse", false),
		rows = fitlerTableRows(table, filter),
		maxPages = Math.ceil(rows.length / perPage),
		currPage = perPage === Infinity ? 0 : Math.min(page, maxPages - 1);

	if (sortBy >= 0) {
		rows.sort(getSorter(cols[sortBy], sortReverse));
	}

	let moved = false,
		clicking = -1,
		downTime = 0;

	return <>
		<Pagination currentPage={currPage} onClick={e => setPage(parseInt((e.target as HTMLElement).dataset["page"] || "0"))} totalPages={maxPages} />
		<table id={id} {...additional}>
			<colgroup>
				{cols.map(() => <col />)}
			</colgroup>
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
				{rows.slice(currPage * perPage, (currPage + 1) * perPage).map(row => <tr onMouseDown={e => {
					if (e.button !== 0) {
						return;
					}

					moved = false;
					window.addEventListener("mousemove", () => moved = true, { "once": true });
					downTime = Date.now();
				}} onMouseUp={e => {
					if (e.button !== 0) {
						return;
					}

					if (!moved || Date.now() - downTime < 100) {
						if (clicking === -1) {
							clicking = window.setTimeout(() => {
								onRowClick(row);
								clicking = -1;
							}, 200);
						} else {
							clearTimeout(clicking);
							clicking = -2;
							window.setTimeout(() => clicking = -1, 200);
						}
					}
				}} {...(rowExtra?.(row) ?? {})}>
					{cols.map(col => <td {...(col.extra?.(row[col.key], row) ?? {})}>{(col.formatter ?? noopFormatter)(row[col.key], row)}</td>)}
				</tr>)}
			</tbody>
		</table>
	</>
};

export default TableComponent;