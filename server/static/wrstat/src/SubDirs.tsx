import type { SubDir } from "./rpc";
import { useEffect, useState } from "react";
import Table from './Table';
import { asDaysAgoStr, formatBytes, formatNumber } from './format';
import RPC from './rpc';

type SubDirParams = {
	id: number;
	path: string;
	isUser: boolean;
	treePath: string;
	justDisktree: boolean;
	setTreePath: (path: string) => void;
}

const pathJoin = (base: string, sub: string) => sub === "." ? base : base + "/" + sub,
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
	sortPath = (a: SubDir, b: SubDir) => stringSort(a.SubDir, b.SubDir),
	sortNumFiles = (a: SubDir, b: SubDir) => a.NumFiles - b.NumFiles,
	sortSizeFiles = (a: SubDir, b: SubDir) => a.SizeFiles - b.SizeFiles,
	sortLastModifed = (a: SubDir, b: SubDir) => new Date(b.LastModified).valueOf() - new Date(a.LastModified).valueOf(),
	SubdirsComponent = ({ id, path, isUser, treePath, setTreePath, justDisktree }: SubDirParams) => {
		const [subdirs, setSubdirs] = useState<SubDir[]>([]);

		useEffect(() => {
			if (id === -1) {
				setSubdirs([]);

				return;
			}

			(isUser ? RPC.getBasedirsUserSubdirs : RPC.getBasedirsGroupSubdirs)(id, path)
				.then(setSubdirs);
		}, [id, path, isUser]);

		if (!subdirs || subdirs.length === 0) {
			return <></>
		}

		return <details open id="subdirs" style={justDisktree ? { display: "none" } : undefined}>
			<summary><h1>Sub-Directories</h1></summary>
			<Table
				id="subDirsTable"
				table={subdirs}
				className="prettyTable"
				onRowClick={(row: SubDir) => setTreePath(pathJoin(path, row.SubDir))}
				caption="Sub Directories"
				rowExtra={row => ({ "className": pathJoin(path, row.SubDir) === treePath ? "selected" : undefined })}
				cols={[
					{
						title: "Path",
						key: "SubDir",
						sortFn: sortPath,
						formatter: (subdir: string) => pathJoin(path, subdir)
					},
					{
						title: "Number of Files",
						key: "NumFiles",
						sortFn: sortNumFiles,
						startReverse: true,
						formatter: formatNumber
					},
					{
						title: "Size",
						key: "SizeFiles",
						extra: size => ({ title: formatNumber(size) + " Bytes" }),
						sortFn: sortSizeFiles,
						startReverse: true,
						formatter: formatBytes
					},
					{
						title: "Last Modified (days)",
						key: "LastModified",
						extra: title => ({ title }),
						sortFn: sortLastModifed,
						startReverse: true,
						formatter: asDaysAgoStr
					},
					{
						title: "File Usage",
						key: "FileUsage",
						formatter: (files: Record<number, number>) => Object.entries(files).sort((a, b) => b[1] - a[1]).map(e => `${fileTypes[parseInt(e[0])]}: ${formatBytes(e[1])}`).join(", ")
					}
				]} />
		</details>
	};

export default SubdirsComponent;