/*******************************************************************************
 * Copyright (c) 2024 Genome Research Ltd.
 *
 * Authors:
 *   Daniel Elia <de7@sanger.ac.uk>
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
import Table from "./Table";


const TreeTableComponent = ({ details, setTreePath }: { details: Child | null, setTreePath: Function }) => {
	if (!details) {
		return <></>
	}
	if (!details.children) {
		return <></>
	}
	return (
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
		/>
	)
}

export default TreeTableComponent;