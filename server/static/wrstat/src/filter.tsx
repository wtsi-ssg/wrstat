import type {Usage} from "./rpc";
import {useState} from "react";
import FilteredTable from "./filteredTable";
import MultiSelect from "./multiselect";

const stringSort = new Intl.Collator().compare;

export default ({groupUsage, userUsage /*, history*/}: {groupUsage: Usage[], userUsage: Usage[] /*, history: Map<string, History[]>*/}) => {
	const [byUser, setBy] = useState(false);
	const [users, setUsers] = useState<string[]>([]);
	const [groups, setGroups] = useState<string[]>([]);
	const [owners, setOwners] = useState<string[]>([]);

	return <>
		<table id="usageFilter">
			<tr>
				<td><label htmlFor="byGroup">By Group</label></td>
				<td><input type="radio" name="by" id="byGroup" checked={!byUser} onChange={e => setBy(!e.target.checked)} /></td>
			</tr>
			<tr>
				<td><label htmlFor="byUser">By User</label></td>
				<td><input type="radio" name="by" id="byUser" checked={byUser} onChange={e => setBy(e.target.checked)} /></td>
			</tr>
			<tr style={byUser ? {} : {display: "none"}}>
				<td><label htmlFor="username">Username</label></td>
				<td><MultiSelect id="username" list={Array.from(new Set(userUsage.map(e => e.Name)).values()).sort(stringSort)} onchange={setUsers} /></td>
			</tr>
			<tr style={byUser ? {display: "none"} : {}}>
				<td><label htmlFor="owners">Owners</label></td>
				<td><MultiSelect id="owners" list={Array.from(new Set(groupUsage.map(e => e.Owner).filter(o => o)).values()).sort(stringSort)} onchange={setOwners} /></td>
			</tr>
			<tr style={byUser ? {display: "none"} : {}}>
				<td><label htmlFor="unix">Unix Group</label></td>
				<td><MultiSelect id="unix" list={Array.from(new Set(groupUsage.map(e => e.Name)).values()).sort(stringSort)} onchange={setGroups} /></td>
			</tr>
		</table>
		<hr />
		<FilteredTable usage={byUser ? userUsage : groupUsage} byUser={byUser} name={byUser ? users : groups} owner={owners} /*history={history}*/ />
	</>
}