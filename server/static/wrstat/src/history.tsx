import type { History } from "./rpc";
import { useEffect, useState } from "react";
import HistoryGraph from "./historyGraph";
import RPC from "./rpc";
import { useSavedState } from "./state";
import { formatBytes, formatLargeNumber, formatNumber } from "./format";
import { exceedDates } from "./trend";

const HistoryComponent = ({ id, path, name, owner, isUser }: { id: number; path: string; name: string; owner: string; isUser: boolean }) => {
	const [inodeHistory, setInodeHistory] = useSavedState("inodeHistory", false),
		[history, setHistory] = useState<History[]>([]),
		[exceedSize, exceedInode] = exceedDates(history);

	useEffect(() => {
		if (id === -1 || path === "") {
			setHistory([]);

			return;
		}

		RPC.getBasedirsHistory(id, path).then(setHistory);
	}, [id, path]);

	if (history.length === 0 || isUser) {
		return <></>
	}

	return <>
		<details open className="boxed">
			<summary>History</summary>
			<h2>Usage | {name} {owner && `(${owner})`} | {path.split("/")[2]}</h2>
			<h3>History/Future Predictions</h3>
			<label htmlFor="sizeHistory">Size History</label><input type="radio" id="sizeHistory" checked={!inodeHistory} onChange={() => setInodeHistory(false)} />
			<label htmlFor="countHistory">Count History</label><input type="radio" id="countHistory" checked={inodeHistory} onChange={() => setInodeHistory(true)} />
			<HistoryGraph history={history.map(h => ({ Date: h.Date, Usage: inodeHistory ? h.UsageInodes : h.UsageSize, Quota: inodeHistory ? h.QuotaInodes : h.QuotaSize }))} width={960} height={500} yFormatter={inodeHistory ? formatLargeNumber : formatBytes} secondaryFormatter={inodeHistory ? formatNumber : (num: number) => formatNumber(num) + " Bytes"} yRounder={inodeHistory ? (maxAmount: number) => {
				const order = Math.pow(10, Math.max(Math.floor(Math.log10(maxAmount)), 1));

				return maxAmount = order * Math.ceil(maxAmount / order);
			} : (maxAmount: number) => 100 * Math.pow(2, Math.ceil(Math.log2(maxAmount / 100)))} />
			{
				!inodeHistory && exceedSize === 0 ? <div className="exceeded">Size Quota has been reached.</div> :
					!inodeHistory && exceedSize !== Infinity ? <div className="exceed">Expected to exceed size quota in {formatNumber(exceedSize)} days.</div> : <></>
			}
			{
				inodeHistory && exceedInode === 0 ? <div className="exceeded">Inode Quota has been reached.</div> :
					inodeHistory && exceedInode !== Infinity ? <div className="exceed">Expected to exceed inode quota in {formatNumber(exceedInode)} days.</div> : <></>
			}
		</details>
	</>
};

export default HistoryComponent;