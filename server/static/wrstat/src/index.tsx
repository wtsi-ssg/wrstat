import type {Usage} from './rpc';
import React from 'react';
import ReactDOM from 'react-dom/client';
import './index.css';
import App from './App';
import Auth from './auth';
import ready from './ready';
import RPC from './rpc';

ready.then(Auth)
.then(username => (username ? Promise.all([
	RPC.getGroupUsageData(),
	RPC.getUserUsageData()
]) : Promise.resolve<[Usage[], Usage[]]>([[], []]))
/*
	.then(([groupUsage, userUsage]) => {
		return new Promise<[Usage[], Usage[], Map<string, History[]>]>(successFn => {
			const history = new Map<string, History[]>();

			let q = Promise.resolve();

			for (const g of groupUsage) {
				q = q.then(() => RPC.getBasedirsHistory(g.GID, g.BaseDir))
				.then(h => {
					history.set(g.Name + "|" + g.BaseDir, h);

					switch(fillQuotaSoon(h)) {
					case false:
						g.status = "OK";

						break;
					case true:
						g.status = "Not OK";

						break;
					default:
						g.status = "Unknown"
					}
				})
			}

			q.then(() => successFn([groupUsage, userUsage, history]));
		});
	})
	*/
	.then(([groupUsage, userUsage /*, history*/]) => ReactDOM.createRoot(document.body).render(
		<React.StrictMode>
			<App username={username} groupUsage={groupUsage} userUsage={userUsage} /*history={history}*/ />
		</React.StrictMode>
	))
)