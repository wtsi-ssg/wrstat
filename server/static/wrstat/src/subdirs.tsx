import type {SubDir} from "./rpc";
import {useEffect, useState} from "react";
import {asDaysAgo, formatBytes, formatNumber} from './format';
import RPC from './rpc';

const pathJoin = (base: string, sub: string) => {
	if (sub === ".") {
		return base;
	}

	return base + "/" + sub;
},
fileTypes = [
	"other",
	"temp",
	"vcf",
	"vcf.gz",
	"bcf",
	"sam",
	"bam",
	"cram",
	"fasta",
	"fastq",
	"fastq.gz",
	"ped/bed",
	"compressed",
	"text",
	"log",
	"dir"
] as const,
stringSort = new Intl.Collator().compare,
sorters = [
	(_a: SubDir, _b: SubDir) => 0,
	(a: SubDir, b: SubDir) => stringSort(a.SubDir, b.SubDir),
	(a: SubDir, b: SubDir) => a.NumFiles - b.NumFiles,
	(a: SubDir, b: SubDir) => a.SizeFiles - b.SizeFiles,
	(a: SubDir, b: SubDir) => new Date(b.LastModified).valueOf() - new Date(a.LastModified).valueOf(),
] as const,
startReverse = [2, 3];

export default ({id, path, isUser, setPath}: {id: number, path: string; isUser: boolean; setPath: (path: string) => void}) => {
	const [subdirs, setSubdirs] = useState<SubDir[]>([]),
	[sortBy, setSortBy] = useState(0),
	[sortReverse, setSortReverse] = useState(false),
	Header = ({sort, name}: {sort: number, name: string}) => <th className={sortBy === sort ? "sort" + (sortReverse ? " reverse" : "") : ""} onClick={() => {
		if (sortBy === sort) {
			setSortReverse(!sortReverse);
		} else {
			setSortBy(sort);
			setSortReverse(startReverse.includes(sort));
		}
	}}>{name}</th>;

	useEffect(() => {
		if (id === -1) {
			setSubdirs([]);

			return;
		}

		(isUser ? RPC.getBasedirsUserSubdirs : RPC.getBasedirsGroupSubdirs)(id, path)
		.then(setSubdirs);
	}, [path, isUser]);

	if (!subdirs || subdirs.length === 0) {
		return <></>
	}

	subdirs.sort(sorters[sortBy]);
	if (sortReverse) {
		subdirs.reverse();
	}


	return <table className="prettyTable historyTable">
		<thead>
			<tr>
				<Header sort={1} name="Path" />
				<Header sort={2} name="Number of Files" />
				<Header sort={3} name="Size" />
				<Header sort={4} name="Last Modified (days)" />
				<th>File Usage</th>
			</tr>
		</thead>
		<tbody>
			{
				subdirs.map(row => <tr onClick={() => setPath(pathJoin(path, row.SubDir))}>
						<td>{pathJoin(path, row.SubDir)}</td>
						<td>{formatNumber(row.NumFiles)}</td>
						<td title={formatNumber(row.SizeFiles)}>{formatBytes(row.SizeFiles)}</td>
						<td>{asDaysAgo(row.LastModified)}</td>
						<td>{Object.entries(row.FileUsage).sort((a, b) => b[1] - a[1]).map(e => `${fileTypes[parseInt(e[0])]}: ${formatBytes(e[1])}`).join(", ")}</td>
				</tr>)
			}
		</tbody>
	</table>
}
