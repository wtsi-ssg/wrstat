import type {SubDir} from "./rpc";
import {useEffect, useState} from "react";
import {asDaysAgo, formatBytes, formatNumber} from './format';
import Table from './table';
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
] as const;

export default ({id, path, isUser, setPath}: {id: number, path: string; isUser: boolean; setPath: (path: string) => void}) => {
	const [subdirs, setSubdirs] = useState<SubDir[]>([]);

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

	return <Table table={subdirs} className="prettyTable historyTable" onRowClick={(row: SubDir) => setPath(pathJoin(path, row.SubDir))} cols={[
		{
			title: "Path",
			key: "SubDir",
			sortFn: sorters[1],
			formatter: (subdir: string) => pathJoin(path, subdir)
		},
		{
			title: "Number of Files",
			key: "NumFiles",
			sortFn: sorters[2],
			startReverse: true,
			formatter: formatNumber
		},
		{
			title: "Size",
			key: "SizeFiles",
			extra: size => ({title: formatNumber(size) + " Bytes"}),
			sortFn: sorters[3],
			startReverse: true,
			formatter: formatBytes
		},
		{
			title: "Last Modified (days)",
			key: "LastModified",
			extra: title => ({title}),
			sortFn: sorters[4],
			startReverse: true,
			formatter: asDaysAgo
		},
		{
			title: "File Usage",
			key: "FileUsage",
			formatter: (files: Record<number, number>) => Object.entries(files).sort((a, b) => b[1] - a[1]).map(e => `${fileTypes[parseInt(e[0])]}: ${formatBytes(e[1])}`).join(", ")
		}
	]} />
}
