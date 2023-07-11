import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import Auth, {logout} from './auth';
import Filter from './filter';
import ready from './ready';
import RPC from './rpc';

const auth = ready.then(Auth),
now = Date.now(),
threeDays = 3 * 86_400_000,
nullDate = "0001-01-01T00:00:00Z",
daysUntilQuotaFull = (date: string) => (new Date(date).valueOf() - now) / threeDays

auth.catch(() => ReactDOM.createRoot(document.body).render(
	<React.StrictMode>
		<div><form action="/login"><input type="submit" value="Login" /></form></div>
	</React.StrictMode>
));

auth.then(username => Promise.all([
	RPC.getGroupUsageData().then(gud => {
		for (const d of gud) {
			d.percentSize = Math.round(10000 * d.UsageSize / d.QuotaSize) / 100;
			d.percentInodes = Math.round(10000 * d.UsageInodes / d.QuotaInodes) / 100;

			let spaceOK = false, filesOK = false;

			if (d.DateNoSpace == nullDate) {
				spaceOK = true
			}
			else {
				spaceOK = daysUntilQuotaFull(d.DateNoSpace) > 3;
			}

			if (d.DateNoFiles == nullDate) {
				filesOK = true
			}
			else {
				filesOK = daysUntilQuotaFull(d.DateNoFiles) > 3;
			}

			d.status = spaceOK && filesOK  ? "OK" :"Not OK";
		}

		return gud;
	}),
	RPC.getUserUsageData(),
	RPC.getChildren({path: "/"})
])
.then(([groupUsage, userUsage, {areas}]) => ReactDOM.createRoot(document.body).render(
	<React.StrictMode>
		<div id="auth">{username} - <button onClick={logout}>Logout</button></div>
		<Filter groupUsage={groupUsage} userUsage={userUsage} areas={areas} />
	</React.StrictMode>
)));