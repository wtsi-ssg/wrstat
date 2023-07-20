import { formatBytes, formatDate, formatNumber } from "./format";
import type { Child } from "./rpc";

const TreedetailsComponent = ({ details, ...rest }: { details: Child | null } & Record<string, any>) => {
	if (!details) {
		return <></>
	}

	return <div id="details" {...rest}>
		{details.path}
		<table>
			<caption>Path Details</caption>
			<tbody>
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
			</tbody>
		</table>
	</div>
};

export default TreedetailsComponent;