import type { History } from "./rpc";
import { useEffect, useLayoutEffect, useState } from "react";
import HistoryGraph from "./HistoryGraph";
import RPC from "./rpc";
import { useSavedState } from "./state";
import { formatBytes, formatLargeNumber, formatNumber } from "./format";
import { exceedDates } from "./trend";
import Tabs from "./Tabs";

const determineGraphWidth = () => Math.max(500, window.innerWidth - 60),
	maxWarningDay = 25,
	daysToday = Math.round(Date.now() / 86_400_000),
	formatSmallBytes = (num: number) => formatNumber(num) + " Bytes",
	formatLog10 = (maxAmount: number) => {
		const order = Math.pow(10, Math.max(Math.floor(Math.log10(maxAmount)), 1));

		return order * Math.ceil(maxAmount / order);
	},
	formatLog2 = (maxAmount: number) => 100 * Math.pow(2, Math.ceil(Math.log2(maxAmount / 100)));

const HistoryWarning = ({ isInodes, history }: { isInodes: boolean, history: History[] }) => {
	const [exceedSize, exceedInode] = exceedDates(history);

	if (isInodes) {
		if (exceedInode === 0) {
			return <div className="exceeded">Inode Quota has been reached.</div>
		}
		if (exceedInode !== Infinity) {
			const prox = 100 * Math.min(maxWarningDay, (exceedInode - daysToday)) / maxWarningDay;
			return <div
				style={{
					[`--warningProx` as any]: prox + "%"
				}}
				className="exceed"
			>Expected to exceed inode quota in {formatNumber(exceedInode - daysToday)} days.</div>
		}

		return <></>
	}

	if (exceedSize === 0) {
		return <div className="exceeded">Size Quota has been reached.</div>
	}
	if (exceedSize !== Infinity) {
		const prox = 100 * Math.min(maxWarningDay, (exceedSize - daysToday)) / maxWarningDay;

		return <div
			style={{ [`--warningProx` as any]: prox + "%" }}
			className="exceed"
		>Expected to exceed size quota in {formatNumber(exceedSize - daysToday)} days.</div>
	}


	return <></>
},
	HistoryComponent = ({ id, path, name, owner, isUser }: { id: number; path: string; name: string; owner: string; isUser: boolean }) => {
		const [inodeHistory, setInodeHistory] = useSavedState("inodeHistory", false),
			[history, setHistory] = useState<History[]>([]),
			[historyWidth, setHistoryWidth] = useState(960);

		useEffect(() => {
			if (id === -1 || path === "") {
				setHistory([]);

				return;
			}

			RPC.getBasedirsHistory(id, path).then(setHistory);
		}, [id, path]);

		useEffect(() => window.addEventListener("resize", () => setHistoryWidth(determineGraphWidth())), []);

		useLayoutEffect(() => setHistoryWidth(determineGraphWidth()));

		if (history.length === 0 || isUser) {
			return <></>
		}

		const graphData = history.map(h => ({
			Date: h.Date,
			Usage: inodeHistory ? h.UsageInodes : h.UsageSize,
			Quota: inodeHistory ? h.QuotaInodes : h.QuotaSize
		}));

		return <>
			<details open className="boxed">
				<summary><h1>Usage History</h1></summary>
				<h2>Usage | {name} {owner && `(${owner})`} | {path.split("/")[2]}</h2>
				<h3>History/Future Predictions</h3>
				<Tabs id="historyTabs" tabs={[
					{
						title: "Size",
						onClick: () => setInodeHistory(false),
						selected: !inodeHistory,
					},
					{
						title: "Count",
						onClick: () => setInodeHistory(true),
						selected: inodeHistory,
					}
				]} />
				<HistoryWarning isInodes={inodeHistory} history={history} />
				<HistoryGraph
					history={graphData}
					width={historyWidth}
					height={500}
					yFormatter={inodeHistory ? formatLargeNumber : formatBytes}
					secondaryFormatter={inodeHistory ? formatNumber : formatSmallBytes}
					yRounder={inodeHistory ? formatLog10 : formatLog2}
				/>
			</details>
		</>
	};

export default HistoryComponent;