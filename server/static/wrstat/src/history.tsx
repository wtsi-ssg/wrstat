import type { History } from "./rpc";
import { useEffect, useState } from "react";
import HistoryGraph from "./historyGraph";
import rpc from "./rpc";
import { useSavedState } from "./state";
import { formatBytes, formatLargeNumber, formatNumber } from "./format";

export default ({ id, path, name, owner, isUser }: { id: number; path: string; name: string; owner: string; isUser: boolean }) => {
	const [inodeHistory, setInodeHistory] = useSavedState("inodeHistory", false),
		[history, setHistory] = useState<History[]>([]);

	useEffect(() => {
		if (id === -1 || path === "") {
			return;
		}

		rpc.getBasedirsHistory(id, path).then(setHistory);
	}, [id, path]);

	return <>
		{
			history.length && !isUser ? <>
				<h2>Usage | {name} {owner && `(${owner})`} | {path.split("/")[2]}</h2>
				<h3>History/Future Predictions</h3>
			</> : <></>
		}
		{
			history.length ? <>
				<label htmlFor="sizeHistory">Size History</label><input type="radio" id="sizeHistory" checked={!inodeHistory} onChange={() => setInodeHistory(false)} />
				<label htmlFor="countHistory">Count History</label><input type="radio" id="countHistory" checked={inodeHistory} onChange={() => setInodeHistory(true)} />
			</> : <></>
		}
		<HistoryGraph history={history.map(h => ({ Date: h.Date, Usage: inodeHistory ? h.UsageInodes : h.UsageSize, Quota: inodeHistory ? h.QuotaInodes : h.QuotaSize }))} width={960} height={500} yFormatter={inodeHistory ? formatLargeNumber : formatBytes} secondaryFormatter={inodeHistory ? formatNumber : (num: number) => formatNumber(num) + " Bytes"} yRounder={inodeHistory ? (maxAmount: number) => {
			const order = Math.pow(10, Math.max(Math.floor(Math.log10(maxAmount)), 1));

			return maxAmount = order * Math.ceil(maxAmount / order)
		} : (maxAmount: number) => 100 * Math.pow(2, Math.ceil(Math.log2(maxAmount / 100)))} />
	</>
}