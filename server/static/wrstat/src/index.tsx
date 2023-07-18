import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import './index.css';
import Auth, { logout } from './auth';
import Filter from './filter';
import ready from './ready';
import RPC from './rpc';

const auth = ready.then(Auth),
	now = Date.now(),
	day = 86_400_000,
	nullDate = "0001-01-01T00:00:00Z",
	daysUntilQuotaFull = (date: string) => (new Date(date).valueOf() - now) / day;

auth.catch(() => createRoot(document.body).render(
	<StrictMode>
		<div id="login"><form action="/login"><input type="submit" value="Sign in via Okta" /></form></div>
	</StrictMode>
));

auth.then(username => Promise.all([
	RPC.getGroupUsageData().then(gud => {
		for (const d of gud) {
			d.percentSize = Math.round(10000 * d.UsageSize / d.QuotaSize) / 100;
			d.percentInodes = Math.round(10000 * d.UsageInodes / d.QuotaInodes) / 100;

			let spaceOK = false, filesOK = false;

			if (d.DateNoSpace === nullDate) {
				spaceOK = true
			}
			else {
				spaceOK = daysUntilQuotaFull(d.DateNoSpace) > 3;
			}

			if (d.DateNoFiles === nullDate) {
				filesOK = true
			}
			else {
				filesOK = daysUntilQuotaFull(d.DateNoFiles) > 3;
			}

			d.status = spaceOK && filesOK ? "OK" : "Not OK";
		}

		return gud;
	}),
	RPC.getUserUsageData(),
	RPC.getChildren({ path: "/" })
])
	.then(([groupUsage, userUsage, { areas }]) => createRoot(document.body).render(
		<StrictMode>
			<svg xmlns="http://www.w3.org/2000/svg" style={{ width: 0, height: 0 }}>
				<symbol id="ok" viewBox="0 0 100 100">
					<circle cx="50" cy="50" r="45" stroke="currentColor" fill="none" stroke-width="10" />
					<path d="M31,50 l13,13 l26,-26" stroke="currentColor" fill="none" stroke-width="10" stroke-linecap="round" stroke-linejoin="round" />
				</symbol>
				<symbol id="notok" viewBox="0 0 100 100">
					<circle cx="50" cy="50" r="45" stroke="currentColor" fill="none" stroke-width="10" />
					<path d="M35,35 l30,30 M35,65 l30,-30" stroke="currentColor" fill="none" stroke-width="10" stroke-linecap="round" />
				</symbol>
			</svg>
			<div id="auth">{username} - <button onClick={logout}>Logout</button></div>
			<Filter groupUsage={groupUsage} userUsage={userUsage} areas={areas} />
		</StrictMode>
	)));


// const stringSort = new Intl.Collator().compare

// export default function App({ groupUsage, userUsage }: { groupUsage: Usage[], userUsage: Usage[] }) {
// 	const users = Array.from(new Set(userUsage.map(e => e.Name)).values()).sort(stringSort)

// 	return <>
// 		<details open className="boxed">
// 			<summary>Filter</summary>
// 			<div className="primaryFilter">
// 				<Filter users={users} />
// 			</div>
// 		</details>
// 	</>
// }