import type { History } from "./rpc";
import { useEffect, useLayoutEffect, useState } from "react";
import HistoryGraph from "./historyGraph";
import RPC from "./rpc";
import { useSavedState } from "./state";
import { formatBytes, formatLargeNumber, formatNumber } from "./format";
import { exceedDates } from "./trend";
import Tabs from "./tabs";

const determineGraphWidth = () => Math.max(500, window.innerWidth - 60),
	maxWarningDay = 25;

const HistoryComponent = ({ id, path, name, owner, isUser }: { id: number; path: string; name: string; owner: string; isUser: boolean }) => {
	const [inodeHistory, setInodeHistory] = useSavedState("inodeHistory", false),
		[history, setHistory] = useState<History[]>([]),
		[historyWidth, setHistoryWidth] = useState(960),
		[exceedSize, exceedInode] = exceedDates(history),
		daysToday = Math.round(Date.now() / 86_400_000);

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

	return <>
		<details open className="boxed">
			<summary>History</summary>
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
			<HistoryGraph history={history.map(h => ({ Date: h.Date, Usage: inodeHistory ? h.UsageInodes : h.UsageSize, Quota: inodeHistory ? h.QuotaInodes : h.QuotaSize }))} width={historyWidth} height={500} yFormatter={inodeHistory ? formatLargeNumber : formatBytes} secondaryFormatter={inodeHistory ? formatNumber : (num: number) => formatNumber(num) + " Bytes"} yRounder={inodeHistory ? (maxAmount: number) => {
				const order = Math.pow(10, Math.max(Math.floor(Math.log10(maxAmount)), 1));

				return maxAmount = order * Math.ceil(maxAmount / order);
			} : (maxAmount: number) => 100 * Math.pow(2, Math.ceil(Math.log2(maxAmount / 100)))} />
			{
				!inodeHistory && exceedSize === 0 ? <div className="exceeded">Size Quota has been reached.</div> :
					!inodeHistory && exceedSize !== Infinity ? <div style={{ [`--warningProx` as any]: (100 * Math.min(maxWarningDay, (exceedSize - daysToday)) / maxWarningDay) + "%" }} className="exceed">Expected to exceed size quota in {formatNumber(exceedSize - daysToday)} days.</div> : <></>
			}
			{
				inodeHistory && exceedInode === 0 ? <div className="exceeded">Inode Quota has been reached.</div> :
					inodeHistory && exceedInode !== Infinity ? <div style={{ [`--warningProx` as any]: (100 * Math.min(maxWarningDay, (exceedInode - daysToday)) / maxWarningDay) + "%" }} className="exceed">Expected to exceed inode quota in {formatNumber(exceedInode - daysToday)} days.</div> : <></>
			}
		</details>
	</>
};

export default HistoryComponent;