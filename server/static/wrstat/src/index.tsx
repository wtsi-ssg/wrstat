import './index.css';
import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import App from './App';
import Auth, { logout } from './auth';
import ready from './ready';
import RPC from './rpc';
import { formatTimeAgo } from './format';

const auth = ready.then(Auth),
	now = Date.now(),
	day = 86_400_000,
	nullDate = "0001-01-01T00:00:00Z",
	daysUntilQuotaFull = (date: string) => (new Date(date).valueOf() - now) / day;

auth.catch(() => createRoot(document.body).render(<StrictMode>
	<div id="login"><form action="/login"><input type="submit" value="Sign in via Okta" /></form></div>
</StrictMode>));

auth.then(username => Promise.all([
	username,
	RPC.getGroupUsageData().then(gud => {
		for (const d of gud) {
			d.percentSize = Math.round(10000 * d.UsageSize / d.QuotaSize) / 100;
			d.percentInodes = Math.round(10000 * d.UsageInodes / d.QuotaInodes) / 100;

			let spaceOK = false, filesOK = false;

			if (d.DateNoSpace === nullDate) {
				spaceOK = true
			} else {
				spaceOK = daysUntilQuotaFull(d.DateNoSpace) > 3;
			}

			if (d.DateNoFiles === nullDate) {
				filesOK = true
			} else {
				filesOK = daysUntilQuotaFull(d.DateNoFiles) > 3;
			}

			d.status = spaceOK && filesOK ? "OK" : "Not OK";
		}

		return gud;
	}),
	RPC.getUserUsageData(),
	RPC.getChildren({ path: "/" })
]))
	.then(([username, groupUsage, userUsage, { areas, timestamp }]) => createRoot(document.body.firstElementChild!).render(<StrictMode>
		<svg xmlns="http://www.w3.org/2000/svg" style={{ width: 0, height: 0 }}>
			<symbol id="ok" viewBox="0 0 100 100">
				<circle cx="50" cy="50" r="45" stroke="currentColor" fill="none" strokeWidth="10" />
				<path d="M31,50 l13,13 l26,-26" stroke="currentColor" fill="none" strokeWidth="10" strokeLinecap="round" strokeLinejoin="round" />
			</symbol>
			<symbol id="notok" viewBox="0 0 100 100">
				<circle cx="50" cy="50" r="45" stroke="currentColor" fill="none" strokeWidth="10" />
				<path d="M35,35 l30,30 M35,65 l30,-30" stroke="currentColor" fill="none" strokeWidth="10" strokeLinecap="round" />
			</symbol>
			<symbol id="lock" viewBox="0 0 100 100">
				<rect rx="15" x="5" y="38" width="90" height="62" fill="currentColor" />
				<path d="M27,40 v-10 a1,1 0,0,1 46,0 v10" fill="none" stroke="currentColor" strokeWidth="12" />
			</symbol>
			<symbol id="emptyDirectory" viewBox="0 0 130 100">
				<path d="M5,15 s0,-5 5,-5 h35 s5,0 5,5 s0,5 5,5 h35 s10,0 10,10 v10 h-65 s-6,0 -10,5 l-20,40 z M5,90 l20,-40 s4,-8 10,-8 h80 s12,0 10,10 l-20,40 s-3,10 -10,10 h-80 s-10,0 -10,-10 z" fill="currentColor" />
				<path d="M103,10 l15,15 M118,10 l-15,15" stroke="currentColor" fill="none" strokeWidth="3" strokeLinecap="round" />
			</symbol>
		</svg>
		<div id="auth">{username} - <button onClick={logout}>Logout</button></div>
		<div id="timestamp" title={timestamp}>Database updated: {formatTimeAgo(timestamp)}</div>
		<App groupUsage={groupUsage} userUsage={userUsage} areas={areas} />
	</StrictMode>));