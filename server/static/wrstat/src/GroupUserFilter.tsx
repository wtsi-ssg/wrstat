import MultiSelect, { type Listener } from "./MultiSelect";
import type { Usage } from "./rpc";

export type GroupUserFilterParams = {
	groupUsage: Usage[];
	userUsage: Usage[];
	areas: Record<string, string[]>;
	users: number[];
	setUsers: (v: number[]) => void;
	groups: number[];
	setGroups: (v: number[]) => void;
	groupNameToIDMap: Map<string, number>;
	groupIDToNameMap: Map<number, string>;
	userNameToIDMap: Map<string, number>;
	userIDToNameMap: Map<number, string>;
}

const stringSort = new Intl.Collator().compare,
	pipes: (Listener | null)[] = [null, null],
	GroupUserFilter = ({
		groupUsage,
		userUsage,
		areas,
		groupNameToIDMap,
		groupIDToNameMap,
		userIDToNameMap,
		userNameToIDMap,
		users,
		setUsers,
		groups,
		setGroups,
		num
	}: GroupUserFilterParams & { num: number }) => {
		const selectedGroups = groups.map(gid => groupIDToNameMap.get(gid) ?? "").sort(stringSort).filter(g => g),
			selectedUsers = users.map(uid => userIDToNameMap.get(uid) ?? "").sort(stringSort).filter(u => u),
			selectedBOMs = Object.entries(areas).map(([bom, groups]) => groups.every(g => groupNameToIDMap.get(g) === undefined || selectedGroups.includes(g)) ? bom : "").filter(b => b).sort(stringSort);

		return <>
			<label htmlFor={`bom_${num}`}>Group Areas</label>
			<MultiSelect
				id={`bom_${num}`}
				list={Object.keys(areas).sort(stringSort)}
				selected={selectedBOMs}
				onchange={(boms, deleted) => {
					if (deleted) {
						for (const pipe of pipes) {
							pipe?.(areas[deleted], true);
						}

						return false;
					}

					const existingGroups = new Set(groups.map(gid => groupIDToNameMap.get(gid) ?? ""));

					for (const bom of boms) {
						for (const g of areas[bom] ?? []) {
							if (groupNameToIDMap.has(g)) {
								existingGroups.add(g);
							}
						}
					}

					for (const pipe of pipes) {
						pipe?.(Array.from(existingGroups), false);
					}

					return false;
				}} />
			<label htmlFor={`unix_${num}`}>Unix Group</label >
			<MultiSelect
				id={`unix_${num}`}
				list={Array.from(new Set(groupUsage.map(e => e.Name)).values()).sort(stringSort)}
				listener={(cb: Listener) => pipes[num] = cb}
				selected={selectedGroups}
				onchange={groups => setGroups(groups.map(groupname => groupNameToIDMap.get(groupname) ?? -1))} />
			<label htmlFor={`username_${num}`}>Username</label>
			<MultiSelect
				id={`username_${num}`}
				list={Array.from(new Set(userUsage.map(e => e.Name)).values()).sort(stringSort)}
				selected={selectedUsers}
				onchange={users => setUsers(users.map(username => userNameToIDMap.get(username) ?? -1))} />
		</>
	}

export default GroupUserFilter;