import type { Usage } from "./rpc";

const escapeQuote = (str: string) => str.replaceAll('"', '""'),
	download = (csv: string, filename: string) => {
		const a = document.createElement("a");
		a.href = URL.createObjectURL(new Blob([csv], { "type": "text/csv;charset=utf-8" }));
		a.setAttribute("download", filename);
		a.click();
	};

export const downloadGroups = (table: Usage[]) => download("PI,Group,Path,Space,SQuota,Inodes,IQuota,LastMod,Status\n" +
	table.map(row => `"${escapeQuote(row.Owner)
		}","${escapeQuote(row.Name)
		}","${escapeQuote(row.BaseDir)
		}",${row.UsageSize
		},${row.QuotaSize
		},${row.UsageInodes
		},${row.QuotaInodes
		},"${escapeQuote(row.Mtime)
		},${row.status
		}"`).join("\r\n"),
	"groups.csv"
),
	downloadUsers = (table: Usage[]) => download("User,Path,Space,Inodes,LastMod\n" +
		table.map(row => `"${escapeQuote(row.Name)
			}","${escapeQuote(row.BaseDir)
			}",${row.UsageSize
			},${row.UsageInodes
			},"${escapeQuote(row.Mtime)
			}"`).join("\r\n"),
		"users.csv"
	);