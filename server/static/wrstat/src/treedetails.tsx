import {formatBytes, formatDate, formatNumber} from "./format";
import type {Child} from "./rpc";

export default ({details}: {details: Child | null}) => {
	if (!details) {
		return <></>
	}

	return <div id="details">
		{details.path}
		<table>
			<tr>
				<th>Size</th>
				<td>{formatBytes(details.size)}</td>
			</tr>
			<tr>
				<th>Count</th>
				<td>{formatNumber(details.count)}</td>
			</tr>
			<tr>
				<th>Accessed</th>
				<td>{formatDate(details.atime)}</td>
			</tr>
			<tr>
				<th>Groups</th>
				<td><div>{details.groups.join(", ")}</div></td>
			</tr>
			<tr>
				<th>Users</th>
				<td><div>{details.users.join(", ")}</div></td>
			</tr>
			<tr>
				<th>Filetypes</th>
				<td><div>{details.filetypes.join(", ")}</div></td>
			</tr>
		</table>
	</div>
};