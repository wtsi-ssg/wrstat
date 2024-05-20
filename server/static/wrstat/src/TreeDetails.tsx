/*******************************************************************************
 * Copyright (c) 2023 Genome Research Ltd.
 *
 * Authors:
 *   Michael Woolnough <mw31@sanger.ac.uk>
 *   Sendu Bala <sb10@sanger.ac.uk>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

import type { Child } from "./rpc";
import { formatBytes, formatDate, formatNumber } from "./format";
import Table from "./Table";

const TreedetailsComponent = ({ details, setTreePath, ...rest }: { details: Child | null, setTreePath: Function } & Record<string, any>) => {
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
					<th aria-label="Files & Directories">Files &amp; Dirs</th>
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

		{details.children && (
			<Table
				table={details.children}
				onRowClick={(child => child.has_children && setTreePath(child.path))}
				id="treeSubDirsTable"
				cols={[{ title: "Sub-Directories", key: "path" },]}
				className={"prettyTable"}
				rowExtra={(child => {
					if (!child.has_children) {
						return {
							"className": "noHover"
						};
					}

					return {};
				})}
			/> ?? <></>)}
	</div>
};

export default TreedetailsComponent;