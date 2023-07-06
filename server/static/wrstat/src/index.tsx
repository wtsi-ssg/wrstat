import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import Auth, {logout} from './auth';
import Filter from './filter';
import ready from './ready';
import RPC from './rpc';

const auth = ready.then(Auth);

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
		}

		return gud;
	}),
	RPC.getUserUsageData(),
	RPC.getChildren({path: "/"})
])
.then(([groupUsage, userUsage, {areas}]) => ReactDOM.createRoot(document.body).render(
	<React.StrictMode>
		<div>
			<div id="auth">{username} - <button onClick={logout}>Logout</button></div>
			<Filter groupUsage={groupUsage} userUsage={userUsage} areas={areas} />
		</div>
	</React.StrictMode>
)));