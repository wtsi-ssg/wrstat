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

import type { Usage } from "./rpc";

const escapeQuote = (str: string) => str.replaceAll('"', '""'),
	download = (csv: string, filename: string) => {
		const a = document.createElement("a");
		a.href = URL.createObjectURL(new Blob([csv], { "type": "text/csv;charset=utf-8" }));
		a.setAttribute("download", filename);
		a.click();
	};

export const downloadGroups = (table: Usage[]) => download(
	"PI,Group,Path,Space,SQuota,Inodes,IQuota,LastMod,Status\n" +
	table.map(row => `"${escapeQuote(row.Owner)
		}","${escapeQuote(row.Name)
		}","${escapeQuote(row.BaseDir)
		}",${row.UsageSize
		},${row.QuotaSize
		},${row.UsageInodes
		},${row.QuotaInodes
		},"${escapeQuote(row.Mtime)
		}",${row.status
		}`).join("\r\n"),
	"groups.csv"
),
	downloadUsers = (table: Usage[]) => download(
		"User,Path,Space,Inodes,LastMod\n" +
		table.map(row => `"${escapeQuote(row.Name)
			}","${escapeQuote(row.BaseDir)
			}",${row.UsageSize
			},${row.UsageInodes
			},"${escapeQuote(row.Mtime)
			}"`).join("\r\n"),
		"users.csv"
	);