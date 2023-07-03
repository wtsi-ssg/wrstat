import type {Usage} from "./rpc";
import {useState} from "react";
import FilteredTable from "./filteredTable";
import MultiSelect from "./multiselect";

const stringSort = new Intl.Collator().compare;

export default ({groupUsage, userUsage, areas /*, history*/}: {groupUsage: Usage[], userUsage: Usage[], areas: Record<string, string[]> /*, history: Map<string, History[]>*/}) => {
	const [byUser, setBy] = useState(false);
	const [users, setUsers] = useState<string[]>([]);
	const [groups, setGroups] = useState<string[]>([]);
	const [boms, setBOMs] = useState<string[]>([]);
	const [owners, setOwners] = useState<string[]>([]);

	return <>
		<div className="treeFilter">
			<label htmlFor="byGroup">By Group</label>
			<input type="radio" name="by" id="byGroup" checked={!byUser} onChange={e => setBy(!e.target.checked)} />
			<label htmlFor="byUser">By User</label>
			<input type="radio" name="by" id="byUser" checked={byUser} onChange={e => setBy(e.target.checked)} />
			<br />
			{!byUser ? [] : <>
				<label htmlFor="username">Username</label>
				<MultiSelect id="username" list={Array.from(new Set(userUsage.map(e => e.Name)).values()).sort(stringSort)} onchange={setUsers} />
			</>}
			{byUser ? [] : <>
				<label htmlFor="owners">Owners</label>
				<MultiSelect id="owners" list={Array.from(new Set(groupUsage.map(e => e.Owner).filter(o => o)).values()).sort(stringSort)} onchange={setOwners} />

				<label htmlFor="unix">Unix Group</label>
				<MultiSelect id="unix" list={Array.from(new Set(groupUsage.map(e => e.Name)).values()).sort(stringSort)} onchange={setGroups} />

				<label htmlFor="bom">Group Areas</label>
				<MultiSelect id="bom" list={Object.keys(areas).sort(stringSort)} onchange={setBOMs} />
			</>}
		</div>
		<FilteredTable usage={byUser ? userUsage : groupUsage} byUser={byUser} name={byUser ? users : groups.concat(boms.map(b => areas[b]).flat())} owner={owners} /*history={history}*/ />
	</>
}
