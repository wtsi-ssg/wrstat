import type {Usage} from "./rpc";
import {useState} from "react";
import FilteredTable from "./filteredTable";

export default ({groupUsage, userUsage /*, history*/}: {groupUsage: Usage[], userUsage: Usage[] /*, history: Map<string, History[]>*/}) => {
	const [byUser, setBy] = useState(false);
	const [name, setName] = useState("");
	const [owner, setOwner] = useState("");

	return (
		<div>
			<label htmlFor="byGroup">By Group</label>
			<input type="radio" name="by" id="byGroup" checked={!byUser} onChange={e => setBy(!e.target.checked)} />
			<br />
			<label htmlFor="byUser">By User</label>
			<input type="radio" name="by" id="byUser" checked={byUser} onChange={e => setBy(e.target.checked)} />
			<br />
			{byUser ? <>
				<datalist id="usernames">
					{
						Array.from(new Set(userUsage.map(e => e.Name)).values()).map(name => <option value={name} />)
					}
				</datalist>
				<label htmlFor="name">Username: </label>
				<input id="name" value={name} onChange={e => setName(e.target.value)} list="usernames" />
			</>
			: <>
				<datalist id="owners">
					{
						Array.from(new Set(groupUsage.map(e => e.Owner).filter(o => o)).values()).map(name => <option value={name} />)
					}
				</datalist>
				<datalist id="groups">
					{
						Array.from(new Set(groupUsage.map(e => e.Name)).values()).map(name => <option value={name} />)
					}
				</datalist>
				<label htmlFor="name">PI Name: </label>
				<input id="name" value={owner} onChange={e => setOwner(e.target.value)} list="owners" />
				<br />
				<label htmlFor="unix">Unix Group: </label>
				<input id="unix" value={name} onChange={e => setName(e.target.value)} list="groups" />
			</>
			}
			
			<hr />
			<FilteredTable usage={byUser ? userUsage : groupUsage} byUser={byUser} name={name} owner={owner} /*history={history}*/ />
		</div>
	)
}