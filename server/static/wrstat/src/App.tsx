import { useEffect, useLayoutEffect, useRef, useState } from "react";
import DiskTreeComponent from "./DiskTree";
import Filter from "./Filter"
import History from './History';
import Scatter from "./Scatter";
import SubDirs from './SubDirs';
import Tabs from "./Tabs";
import { fitlerTableRows } from "./Table";
import UsageTable from "./UsageTable";
import { asDaysAgo } from "./format";
import type { Usage } from "./rpc"
import { clearState, useSavedState } from "./state";

const groupNameToIDMap = new Map<string, number>(),
	userNameToIDMap = new Map<string, number>(),
	userIDToNameMap = new Map<number, string>(),
	groupIDToNameMap = new Map<number, string>(),
	calculateScatterWidth = (div: HTMLDivElement) => Math.min(Math.max(300, parseInt(getComputedStyle(div).gridTemplateColumns.split(" ")[1]) - 40), 1500),
	App = ({ groupUsage, userUsage, areas }: { groupUsage: Usage[], userUsage: Usage[], areas: Record<string, string[]> }) => {
		if (groupNameToIDMap.size === 0) {
			for (const { GID, Name } of groupUsage) {
				groupNameToIDMap.set(Name, GID);
				groupIDToNameMap.set(GID, Name);
			}

			for (const { UID, Name } of userUsage) {
				userNameToIDMap.set(Name, UID);
				userIDToNameMap.set(UID, Name);
			}
		}

		const [byUser, setByUser] = useSavedState("byUser", false),
			[just, setJust] = useSavedState("just", ""),
			[users, setUsers] = useSavedState<number[]>("users", []),
			[groups, setGroups] = useSavedState<number[]>("groups", []),
			[owners, setOwners] = useSavedState<string[]>("owners", []),
			[axisMinSize, setAxisMinSize] = useSavedState("filterMinSize", -Infinity),
			[axisMaxSize, setAxisMaxSize] = useSavedState("filterMaxSize", Infinity),
			[axisMinDaysAgo, setAxisMinDaysAgo] = useSavedState("filterMinDaysAgo", -Infinity),
			[axisMaxDaysAgo, setAxisMaxDaysAgo] = useSavedState("filterMaxDaysAgo", Infinity),
			[savedMinSize, setSavedMinSize] = useSavedState("minSize", -Infinity),
			[savedMaxSize, setSavedMaxSize] = useSavedState("maxSize", Infinity),
			[savedMinDaysAgo, setSavedMinDaysAgo] = useSavedState("minDaysAgo", -Infinity),
			[savedMaxDaysAgo, setSavedMaxDaysAgo] = useSavedState("maxDaysAgo", Infinity),
			[filterMinDaysAgo, setFilterMinDaysAgo] = useState(savedMinDaysAgo),
			[filterMaxDaysAgo, setFilterMaxDaysAgo] = useState(savedMaxDaysAgo),
			[filterMinSize, setFilterMinSize] = useState(savedMinSize),
			[filterMaxSize, setFilterMaxSize] = useState(savedMaxSize),
			[selectedDir, setSelectedDir] = useSavedState("selectedDir", ""),
			[selectedID, setSelectedID] = useSavedState("selectedID", -1),
			[treePath, setTreePath] = useSavedState("treePath", "/"),
			primaryFilter = useRef<HTMLDivElement>(null),
			[scaleSize, setScaleSize] = useSavedState("scaleSize", false),
			[scaleDays, setScaleDays] = useSavedState("scaleDays", false),
			[scatterWidth, setScatterWidth] = useState(300),
			usage = byUser ? userUsage : groupUsage,
			selectedRow = usage.filter(u => (byUser ? u.UID : u.GID) === selectedID && u.BaseDir === selectedDir)[0],
			baseFilter = byUser ? {
				UID: users,
				GIDs: (gids: number[]) => groups.length ? gids.some(gid => groups.includes(gid)) : true,
			} : {
				GID: groups,
				UIDs: (uids: number[]) => users.length ? uids.some(uid => users.includes(uid)) : true,
				Owner: owners
			},
			filter = Object.assign({
				UsageSize: { min: Math.max(filterMinSize, axisMinSize), max: Math.min(filterMaxSize, axisMaxSize) },
				Mtime: (mtime: string) => {
					const daysAgo = asDaysAgo(mtime);

					return daysAgo >= Math.max(filterMinDaysAgo, axisMinDaysAgo) && daysAgo <= Math.min(filterMaxDaysAgo, axisMaxDaysAgo);
				}
			}, baseFilter),
			scatterFilter = Object.assign({
				UsageSize: { min: axisMinSize, max: axisMaxSize },
				Mtime: (mtime: string) => {
					const daysAgo = asDaysAgo(mtime);

					return daysAgo >= axisMinDaysAgo && daysAgo <= axisMaxDaysAgo;
				}
			}, baseFilter),
			preview = savedMinSize !== filterMinSize || savedMaxSize !== filterMaxSize || savedMinDaysAgo !== filterMinDaysAgo || savedMaxDaysAgo !== filterMaxDaysAgo,
			guf = {
				groupUsage,
				userUsage,
				areas,
				groupNameToIDMap,
				groupIDToNameMap,
				userNameToIDMap,
				userIDToNameMap,
				users,
				setUsers,
				groups,
				setGroups
			},
			justDisktree = just.toLowerCase() === "disktree";

		if (!preview && selectedDir !== "" && selectedID !== -1 && fitlerTableRows(usage.filter(u => (byUser ? u.UID : u.GID) === selectedID && u.BaseDir === selectedDir), filter).length === 0) {
			setSelectedDir("");
			setSelectedID(-1);
		}

		useEffect(() => {
			setFilterMinSize(savedMinSize);
			setFilterMaxSize(savedMaxSize);
			setFilterMinDaysAgo(savedMinDaysAgo);
			setFilterMaxDaysAgo(savedMaxDaysAgo);
		}, [savedMinDaysAgo, savedMaxDaysAgo, savedMinSize, savedMaxSize]);

		useEffect(() => {
			window.addEventListener("resize", () => {
				if (primaryFilter.current) {
					setScatterWidth(calculateScatterWidth(primaryFilter.current));
				}
			});
		}, []);

		useLayoutEffect(() => {
			if (primaryFilter.current) {
				setScatterWidth(calculateScatterWidth(primaryFilter.current));
			}
		}, []);

		return <>
			<Tabs id="mainTabs" tabs={[
				{
					title: "Group Base Directories",
					onClick: () => {
						clearState();
						setByUser(false);
						setJust("");
					},
					selected: !justDisktree && !byUser
				},
				{
					title: "User Base Directories",
					onClick: () => {
						clearState();
						setByUser(true);
						setJust("");
					},
					selected: !justDisktree && byUser
				},
				{
					title: "Just Disktree",
					onClick: () => setJust("disktree"),
					selected: justDisktree
				}
			]} />
			<details open style={justDisktree ? { display: "none" } : undefined}>
				<summary><h1>Filter</h1></summary>
				<div className="primaryFilter" ref={primaryFilter}>
					<Filter {...({
						usage: byUser ? userUsage : groupUsage,
						groupUsage,
						axisMinSize, setAxisMinSize,
						axisMaxSize, setAxisMaxSize,
						axisMinDaysAgo, setAxisMinDaysAgo,
						axisMaxDaysAgo, setAxisMaxDaysAgo,
						scaleSize, setScaleSize,
						scaleDays, setScaleDays,
						guf,
						owners,
						setOwners,
						byUser,
						setByUser
					})} />
					<Scatter
						width={scatterWidth}
						height={400}
						data={fitlerTableRows(byUser ? userUsage : groupUsage, scatterFilter)}
						logX={scaleDays}
						logY={scaleSize}
						minX={savedMinDaysAgo}
						maxX={savedMaxDaysAgo}
						minY={savedMinSize}
						maxY={savedMaxSize}
						isSelected={(u: any) => (byUser ? u.UID : u.GID) === selectedID && u.BaseDir === selectedDir}
						setLimits={(minS, maxS, minD, maxD) => {
							setSavedMinSize(minS);
							setSavedMaxSize(maxS);
							setSavedMinDaysAgo(minD);
							setSavedMaxDaysAgo(maxD);
							setFilterMinSize(minS);
							setFilterMaxSize(maxS);
							setFilterMinDaysAgo(minD);
							setFilterMaxDaysAgo(maxD);
						}} previewLimits={(minS, maxS, minD, maxD) => {
							setFilterMinSize(minS);
							setFilterMaxSize(maxS);
							setFilterMinDaysAgo(minD);
							setFilterMaxDaysAgo(maxD);
						}} />
				</div>
			</details>
			<UsageTable
				{...{
					userMap: userIDToNameMap,
					groupMap: groupIDToNameMap,
					usage,
					byUser,
					selectedID,
					setSelectedID,
					selectedDir,
					setSelectedDir,
					setTreePath,
					filter,
					justDisktree
				}} />

			<History
				id={selectedID}
				path={selectedDir}
				isUser={byUser}
				name={selectedRow?.Name}
				owner={selectedRow?.Owner}
				justDisktree={justDisktree}
			/>
			<SubDirs
				id={selectedID}
				path={selectedDir}
				isUser={byUser}
				treePath={treePath}
				setTreePath={setTreePath}
				justDisktree={justDisktree}
			/>
			{justDisktree ? <DiskTreeComponent
				{...{
					userMap: userIDToNameMap,
					groupMap: groupIDToNameMap,
					treePath,
					setTreePath,
					guf,
					filter
				}} /> :
				<details open>
					<summary><h1>Disktree</h1></summary>
					<DiskTreeComponent
						{...{
							userMap: userIDToNameMap,
							groupMap: groupIDToNameMap,
							treePath,
							setTreePath,
							guf,
							filter
						}} />
				</details>
			}
		</>
	};

export default App;